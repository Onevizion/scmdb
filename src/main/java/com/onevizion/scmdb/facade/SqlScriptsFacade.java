package com.onevizion.scmdb.facade;

import com.onevizion.scmdb.AppArguments;
import com.onevizion.scmdb.dao.SqlScriptDaoOra;
import com.onevizion.scmdb.vo.SqlScript;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.onevizion.scmdb.vo.ScriptType.COMMIT;
import static com.onevizion.scmdb.vo.ScriptType.ROLLBACK;

@Component
public class SqlScriptsFacade {
    @Resource
    private SqlScriptDaoOra sqlScriptDaoOra;

    @Resource
    private AppArguments appArguments;

    private final static String EXEC_FOLDER_NAME = "EXECUTE_ME";
    private final static String ERROR_MSG_COMMIT_DELETED_WITHOUT_ROLLBACK = "Following scripts were deleted but it's rollbacks are still here. Remove rollbacks scripts or restore deleted scripts and then run scmdb again.";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public List<SqlScript> getNewScripts() {
        logger.debug("Searching new scripts in [{}]", appArguments.getScriptDirectory().getAbsolutePath());

        Map<String, SqlScript> savedScripts = sqlScriptDaoOra.readMap();
        List<SqlScript> scriptsInDir = createScriptsFromFiles();

        scriptsInDir.stream()
                    .parallel()
                    .filter(this::isDevScript)
                    .forEach(script -> logger.info("Dev script [" + script.getName() + "] was ignored"));

        return scriptsInDir.stream()
                           .parallel()
                           .filter(script -> !savedScripts.containsKey(script.getName()))
                           .filter(script -> !isDevScript(script))
                           .collect(Collectors.toList());
    }

    private boolean isDevScript(SqlScript script) {
        String[] parts = script.getName().split("_");
        return parts.length <= 1 || !NumberUtils.isDigits(parts[0]);
    }


    @Transactional
    public List<SqlScript> getScriptsToExec() {
        logger.debug("Searching new scripts in [{}]", appArguments.getScriptDirectory().getAbsolutePath());

        Map<String, SqlScript> dbScripts = sqlScriptDaoOra.readMap();
        List<File> scriptFiles = (List<File>) FileUtils.listFiles(appArguments.getScriptDirectory(), new String[]{"sql"}, false);
        List<SqlScript> scriptsInDir = createScriptsFromFiles();

        for (SqlScript script : scriptsInDir) {
            if (dbScripts.containsKey(script.getName())) {
                SqlScript savedScript = dbScripts.get(script.getName());
                if (!script.getFileHash().equals(savedScript.getFileHash())) {
                    logger.warn("Script file was changed [{}]", script.getName());
                    savedScript.setFileHash(script.getFileHash());
                    sqlScriptDaoOra.update(savedScript);
                }
            }
        }

        logger.debug("Searching deleted scripts in [{}]", appArguments.getScriptDirectory().getAbsolutePath());
        Collection<SqlScript> deletedScripts = CollectionUtils.subtract(dbScripts.values(), scriptsInDir);
        List<SqlScript> rollbacksToExec = new ArrayList<>();
        List<Long> deleteScriptIds = new ArrayList<>();
        for (SqlScript deletedScript : deletedScripts) {
            if (deletedScript.getType() == ROLLBACK) {
                deleteScriptIds.add(deletedScript.getId());
            } else if (deletedScript.getType() == COMMIT && dbScripts.containsKey(deletedScript.getRollbackName())) {
                rollbacksToExec.add(dbScripts.get(deletedScript.getRollbackName()));
                deleteScriptIds.add(deletedScript.getId());
            }
        }
        if (!deleteScriptIds.isEmpty()) {
            logger.debug("Deleting missed scripts form db");
            sqlScriptDaoOra.deleteByIds(deleteScriptIds);
        }

        List<SqlScript> newScripts = scriptsInDir.stream()
                                                 .filter(script -> script.getType() == ROLLBACK)
                                                 .filter(script -> dbScripts.containsKey(script.getName()))
                                                 .collect(Collectors.toList());

        return copyRollbacksToExecDir(newScripts, rollbacksToExec);
    }

    public List<SqlScript> copyRollbacksToExecDir(List<SqlScript> newScripts, List<SqlScript> rollbacks) {
        File execDir = createExecDir();

        for (SqlScript vo : rollbacks) {
            File f = new File(execDir.getAbsolutePath() + File.separator + vo.getName());
            try {
                logger.debug("Creating rollback script [{}]", f.getAbsolutePath());
                FileUtils.writeStringToFile(f, vo.getText());
                logger.info("Execute manually rollback script: [{}]", f.getAbsolutePath());
            } catch (IOException e) {
                logger.error("Can't create file [{}]", f.getAbsolutePath(), e);
                throw new RuntimeException(e);
            }
        }

        for (SqlScript script : newScripts) {
            if (script.getType() == ROLLBACK) {
                continue;
            }
            File srcFile = new File(appArguments.getScriptDirectory()
                                                .getAbsolutePath() + File.separator + script.getName());
            File destFile = new File(execDir.getAbsolutePath() + File.separator + script.getName());
            try {
                logger.debug("Copying new script [{}]", destFile.getAbsolutePath());
                FileUtils.copyFile(srcFile, destFile);
                script.setFile(destFile);
            } catch (IOException e) {
                logger.error("Can't copy file [{}]", srcFile.getAbsolutePath(), e);
                throw new RuntimeException(e);
            }
        }

        return newScripts;
    }

    public boolean isFirstRun() {
        return sqlScriptDaoOra.readCount().equals(0L);
    }

    private File createExecDir() {
        File execDir = new File(appArguments.getScriptDirectory()
                                            .getAbsolutePath() + File.separator + EXEC_FOLDER_NAME);
        if (execDir.exists()) {
            try {
                FileUtils.deleteDirectory(execDir);
            } catch (IOException e) {
                logger.error("Can't delete directory by path {" + execDir.getAbsolutePath() + "}", e);
            }
        }
        return execDir;
    }

    private List<SqlScript> createScriptsFromFiles() {
        List<File> scriptFiles = (List<File>) FileUtils.listFiles(appArguments.getScriptDirectory(), new String[]{"sql"}, false);
        return scriptFiles.stream()
                          .map(SqlScript::create)
                          .collect(Collectors.toList());
    }

    private Map<String, SqlScript> createScriptsMapFromFiles() {
        List<File> scriptFiles = (List<File>) FileUtils.listFiles(appArguments.getScriptDirectory(), new String[]{"sql"}, false);
        return scriptFiles.stream()
                          .collect(Collectors.toMap(File::getName, SqlScript::create));
    }


    public List<SqlScript> getUpdatedScripts() {
        List<SqlScript> updatedScripts = new ArrayList<>();

        Map<String, SqlScript> dbScripts = sqlScriptDaoOra.readMap();
        List<SqlScript> scriptsInDir = createScriptsFromFiles();

        for (SqlScript scriptInDir : scriptsInDir) {
            if (!dbScripts.containsKey(scriptInDir.getName())) {
                continue;
            }
            SqlScript savedScript = dbScripts.get(scriptInDir.getName());
            if (!scriptInDir.getFileHash().equals(savedScript.getFileHash())) {
                savedScript.setFileHash(scriptInDir.getFileHash());
                updatedScripts.add(savedScript);
            }
        }

        return updatedScripts;
    }

    public void batchUpdate(List<SqlScript> updatedScripts) {
        sqlScriptDaoOra.batchUpdate(updatedScripts);
    }

    public void batchCreate(List<SqlScript> scripts) {
        sqlScriptDaoOra.batchCreate(scripts);
    }

    public Map<String, SqlScript> getDeletedScriptsMap() {
        Map<String, SqlScript> dbScripts = sqlScriptDaoOra.readMap();
        Map<String, SqlScript> scriptsInDir = createScriptsMapFromFiles();

        logger.debug("Searching deleted scripts in [{}]", appArguments.getScriptDirectory().getAbsolutePath());
        Map<String, SqlScript> deletedScripts = dbScripts.values().stream()
                                                         .filter(dbScript -> !scriptsInDir.containsKey(dbScript.getName()))
                                                         .collect(Collectors.toMap(SqlScript::getName, Function.identity()));

        List<SqlScript> commitsDeletedWithoutRollbacks =
                deletedScripts.values().stream()
                              .filter(script -> script.getType() == COMMIT)
                              .filter(script -> !deletedScripts.containsKey(script.getRollbackName()))
                              .collect(Collectors.toList());
        if (!commitsDeletedWithoutRollbacks.isEmpty()) {
            logger.error(ERROR_MSG_COMMIT_DELETED_WITHOUT_ROLLBACK);
            commitsDeletedWithoutRollbacks.forEach(script -> logger.error("Deleted script: [{}], rollback: [{}]",
                    script.getName(), script.getRollbackName()));
            throw new RuntimeException("");
        }

        return deletedScripts;
    }

    public void deleteAll(Collection<SqlScript> scripts) {
        sqlScriptDaoOra.deleteByIds(scripts.stream()
                                          .map(SqlScript::getId)
                                          .collect(Collectors.toSet()));
    }

    public void create(SqlScript script) {
        sqlScriptDaoOra.create(script);
    }

    public void createAllFromDirectory() {
        sqlScriptDaoOra.batchCreate(createScriptsFromFiles());
    }
}