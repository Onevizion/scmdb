package com.onevizion.scmdb.facade;

import com.onevizion.scmdb.AppArguments;
import com.onevizion.scmdb.dao.DbScriptDaoOra;
import com.onevizion.scmdb.vo.SqlScript;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.onevizion.scmdb.vo.ScriptType.COMMIT;

@Component
public class DbScriptFacade {
    @Resource
    private DbScriptDaoOra sqlScriptDaoOra;

    @Resource
    private AppArguments appArguments;

    private final static String EXEC_FOLDER_NAME = "EXECUTE_ME";
    private final static String ERROR_MSG_COMMIT_DELETED_WITHOUT_ROLLBACK = "Following scripts were deleted but it's rollbacks are still here. Remove rollbacks scripts or restore deleted scripts and then run scmdb again.";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private File execDir;
    private List<SqlScript> scriptsInDir;

    public void init() {
        execDir = new File(appArguments.getScriptsDirectory().getAbsolutePath() + File.separator + EXEC_FOLDER_NAME);
        scriptsInDir = createScriptsFromFiles();
    }

    public List<SqlScript> getNewScripts() {
        logger.debug("Searching new scripts in [{}]", appArguments.getScriptsDirectory().getAbsolutePath());

        Map<String, SqlScript> savedScripts = sqlScriptDaoOra.readMap();

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

    public void copyRollbacksToExecDir(List<SqlScript> rollbacks) {
        for (SqlScript rollback : rollbacks) {
            copyRollbackToExecDir(rollback);
        }
    }

    public void copyRollbackToExecDir(SqlScript rollback) {
        File rollBackFile = new File(execDir.getAbsolutePath() + File.separator + rollback.getName());
        rollback.setFile(rollBackFile);
        try {
            logger.debug("Creating rollback script [{}]", rollBackFile.getAbsolutePath());
            FileUtils.writeStringToFile(rollBackFile, rollback.getText(), "UTF-8");
        } catch (IOException e) {
            logger.error("Can't create file [{}]", rollBackFile.getAbsolutePath(), e);
            throw new RuntimeException(e);
        }
    }

    public boolean isFirstRun() {
        return sqlScriptDaoOra.readCount().equals(0L);
    }

    public void cleanExecDir() {
        if (execDir.exists()) {
            try {
                FileUtils.deleteDirectory(execDir);
            } catch (IOException e) {
                throw new RuntimeException("Can't delete execution directory by path [" + execDir.getAbsolutePath() + "]", e);
            }
        }
    }

    private List<SqlScript> createScriptsFromFiles() {
        List<File> scriptFiles = (List<File>) FileUtils.listFiles(appArguments.getScriptsDirectory(), new String[]{"sql"}, false);
        return scriptFiles.stream()
                          .map(SqlScript::create)
                          .collect(Collectors.toList());
    }

    public List<SqlScript> getUpdatedScripts() {
        List<SqlScript> updatedScripts = new ArrayList<>();
        Map<String, SqlScript> dbScripts = sqlScriptDaoOra.readMap();

        for (SqlScript scriptInDir : scriptsInDir) {
            if (!dbScripts.containsKey(scriptInDir.getName())) {
                continue;
            }
            SqlScript savedScript = dbScripts.get(scriptInDir.getName());
            if (!scriptInDir.getFileHash().equals(savedScript.getFileHash())) {
                scriptInDir.setId(savedScript.getId());
                updatedScripts.add(scriptInDir);
            }
        }

        return updatedScripts;
    }

    public void batchUpdate(List<SqlScript> updatedScripts) {
        sqlScriptDaoOra.batchUpdate(updatedScripts);
    }

    public void batchCreate(List<SqlScript> scripts) {
        sqlScriptDaoOra.createAll(scripts);
    }

    public Map<String, SqlScript> getDeletedScriptsMap() {
        Map<String, SqlScript> dbScripts = sqlScriptDaoOra.readMap();
        Map<String, SqlScript> scriptsInDirMap = scriptsInDir.stream()
                                                             .collect(Collectors.toMap(SqlScript::getName, Function.identity()));


        logger.debug("Searching deleted scripts in [{}]", appArguments.getScriptsDirectory().getAbsolutePath());
        Map<String, SqlScript> deletedScripts = dbScripts.values().stream()
                                                         .filter(dbScript -> !scriptsInDirMap.containsKey(dbScript.getName()))
                                                         .collect(Collectors.toMap(SqlScript::getName, Function.identity()));

        List<SqlScript> commitsDeletedWithoutRollbacks =
                deletedScripts.values().stream()
                              .filter(script -> script.getType() == COMMIT)
                              .filter(script -> scriptsInDirMap.containsKey(script.getRollbackName()))
                              .filter(script -> !deletedScripts.containsKey(script.getRollbackName()))
                              .collect(Collectors.toList());
        if (!commitsDeletedWithoutRollbacks.isEmpty()) {
            logger.error(ERROR_MSG_COMMIT_DELETED_WITHOUT_ROLLBACK);
            commitsDeletedWithoutRollbacks.forEach(script -> logger.error("Deleted script: [{}], rollback: [{}]",
                    script.getName(), script.getRollbackName()));
            System.exit(0);
        }

        return deletedScripts;
    }

    public void deleteAll(Collection<SqlScript> scripts) {
        sqlScriptDaoOra.deleteByIds(scripts.stream()
                                           .map(SqlScript::getId)
                                           .collect(Collectors.toList()));
    }

    public void create(SqlScript script) {
        sqlScriptDaoOra.create(script);
    }

    public void createAllFromDirectory() {
        sqlScriptDaoOra.createAll(createScriptsFromFiles());
    }

    public void delete(Long id) {
        sqlScriptDaoOra.delete(id);
    }

    public void copyScriptsToExecDir(List<SqlScript> scripts) {
        for (SqlScript script : scripts) {
            File srcFile = new File(appArguments.getScriptsDirectory()
                                                .getAbsolutePath() + File.separator + script.getName());
            File destFile = new File(execDir.getAbsolutePath() + File.separator + script.getName());
            try {
                logger.debug("Copying new script [{}]", destFile.getAbsolutePath());
                FileUtils.copyFile(srcFile, destFile);
            } catch (IOException e) {
                logger.error("Can't copy file [{}]", srcFile.getAbsolutePath(), e);
                throw new RuntimeException(e);
            }
        }
    }

    public boolean isScriptTableExist() {
        try {
            return sqlScriptDaoOra.isScriptTableExist();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    public void checkDbConnection() throws SQLException {
        sqlScriptDaoOra.checkDbConnection();
    }
}