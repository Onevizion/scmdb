package com.onevizion.scmdb.facade;

import com.onevizion.scmdb.AppArguments;
import com.onevizion.scmdb.ColorLogger;
import com.onevizion.scmdb.ResourceResolveUtils;
import com.onevizion.scmdb.dao.DbScriptDaoOra;
import com.onevizion.scmdb.exception.ScmdbException;
import com.onevizion.scmdb.vo.SqlScript;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.onevizion.scmdb.vo.ScriptType.COMMIT;
import static com.onevizion.scmdb.vo.ScriptType.ROLLBACK;

@Component
public class DbScriptFacade {

    private static final int MAX_DEVELOPMENT_ORDER_NUMBER = 100;
    private static final String EXEC_FOLDER_NAME = "EXECUTE_ME";
    private static final String ERROR_MSG_COMMIT_DELETED_WITHOUT_ROLLBACK = "Following scripts were deleted but it's rollbacks are still here. Remove rollbacks scripts or restore deleted scripts and then run scmdb again.";

    @Autowired
    private DbScriptDaoOra sqlScriptDaoOra;

    @Autowired
    private AppArguments appArguments;

    @Autowired
    private ColorLogger logger;

    private File execDir;
    private List<SqlScript> scriptsInDir;

    public void init() {
        execDir = appArguments.getScriptsDirectory() != null
                ? new File(appArguments.getScriptsDirectory().getAbsolutePath(), EXEC_FOLDER_NAME)
                : createTempDirectory();

        scriptsInDir = createScriptsFromResources(appArguments.isReadAllFilesContent());
    }

    private File createTempDirectory() {
        try {
            File tempDirectory = Files.createTempDirectory(EXEC_FOLDER_NAME).toFile();
            FileUtils.forceDeleteOnExit(tempDirectory);

            return tempDirectory;
        } catch (IOException e) {
            throw new RuntimeException("Unable to create temporarily directory", e);
        }
    }

    public List<SqlScript> getNotExecutedScripts() {
        Map<String, SqlScript> savedScripts = sqlScriptDaoOra.readMap();
        List<SqlScript> newScripts = scriptsInDir.stream()
                                                 .filter(script -> !savedScripts.containsKey(script.getName()))
                                                 .collect(Collectors.toList());
        if (!appArguments.isReadAllFilesContent()) {
            newScripts.forEach(SqlScript::loadContentFromFile);
        }
        return newScripts;
    }

    public Map<String, SqlScript> getSavedScriptsMap() {
        return sqlScriptDaoOra.readMap();
    }

    public List<SqlScript> getDevelopmentRollbackScriptsForSavedCommits(Map<String, SqlScript> savedScripts) {
        return scriptsInDir.stream()
                           .filter(script -> script.getType() == ROLLBACK)
                           .filter(script -> script.getOrderNumber() != null)
                           .filter(script -> script.getOrderNumber() < MAX_DEVELOPMENT_ORDER_NUMBER)
                           .filter(rollback -> savedScripts.containsKey(rollback.getName()))
                           .filter(rollback -> savedScripts.containsKey(rollback.getCommitName()))
                           .peek(rollback -> rollback.setId(savedScripts.get(rollback.getName()).getId()))
                           .collect(Collectors.toList());
    }

    public List<SqlScript> getDevelopmentScripts() {
        List<SqlScript> newScripts = scriptsInDir.stream()
                                                 .filter(s -> s.getOrderNumber() < MAX_DEVELOPMENT_ORDER_NUMBER)
                                                 .collect(Collectors.toList());
        if (!appArguments.isReadAllFilesContent()) {
            newScripts.forEach(SqlScript::loadContentFromFile);
        }
        return newScripts;
    }

    private boolean isIgnoredScript(SqlScript script) {
        String[] parts = script.getName().split("_");
        boolean isDevScript = parts.length <= 1 || !StringUtils.isNumeric(parts[0]);
        if (isDevScript) {
            logger.info("Dev script [" + script.getName() + "] was ignored");
        }
        return isDevScript;
    }

    public void copyRollbacksToExecDir(List<SqlScript> rollbacks) {
        for (SqlScript rollback : rollbacks) {
            copyRollbackToExecDir(rollback);
        }
    }

    public void copyRollbackToExecDir(SqlScript rollback) {
        File rollBackFile = new File(execDir.getAbsolutePath() + File.separator + rollback.getName());
        rollback.setResource(new FileSystemResource(rollBackFile));
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

    private List<SqlScript> createScriptsFromResources(boolean readAllScriptsContent) {
        return ResourceResolveUtils.resolveScriptResources(appArguments.getScriptsDirectory())
                                   .stream()
                                   .map(resource -> {
                                       try {
                                           return SqlScript.create(resource, readAllScriptsContent);
                                       } catch (IOException e) {
                                           throw new RuntimeException("Unable to create SqlScript instance for [" + resource + "]", e);
                                       }
                                   })
                                   .filter(s -> !isIgnoredScript(s))
                                   .sorted()
                                   .toList();
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
            commitsDeletedWithoutRollbacks.forEach(script -> logger.error("Deleted script: [{}], rollback: [{}]",
                    script.getName(), script.getRollbackName()));
            throw new ScmdbException(ERROR_MSG_COMMIT_DELETED_WITHOUT_ROLLBACK);
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
        sqlScriptDaoOra.createAll(createScriptsFromResources(true));
    }

    public void delete(Long id) {
        sqlScriptDaoOra.delete(id);
    }

    public void copyScriptsToExecDir(List<SqlScript> scripts) {
        for (SqlScript script : scripts) {
            File destFile = new File(execDir.getAbsolutePath() + File.separator + script.getName());

            try (InputStream inputStream = script.getResource().getInputStream()) {
                logger.debug("Copying new script [{}]", destFile.getAbsolutePath());
                FileUtils.copyInputStreamToFile(inputStream, destFile);
            } catch (IOException e) {
                logger.error("Can't copy script to file [{}]", destFile.getAbsolutePath(), e);
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

    public void checkDbConnection() {
        sqlScriptDaoOra.checkDbConnection();
    }
}