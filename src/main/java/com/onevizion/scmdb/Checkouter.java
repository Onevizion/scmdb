package com.onevizion.scmdb;

import com.onevizion.scmdb.facade.DdlFacade;
import com.onevizion.scmdb.facade.SqlScriptsFacade;
import com.onevizion.scmdb.vo.ScriptStatus;
import com.onevizion.scmdb.vo.ScriptType;
import com.onevizion.scmdb.vo.SqlScript;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.onevizion.scmdb.vo.ScriptType.ROLLBACK;

public class Checkouter {
    private static final Logger logger = LoggerFactory.getLogger(Checkouter.class);
    private static final String SCRIPT_EXECUTION_ERROR_MESSAGE = "Fix and execute manually script [{}] and then run scmdb again to execute other scripts.";

    @Resource
    private SqlScriptsFacade scriptsFacade;

    @Resource
    private DdlFacade ddlFacade;

    @Resource
    private AppArguments appArguments;

    @Resource
    private SqlScriptExecutor scriptExecutor;

    public void updateDb() {
        logger.info("Checking out your database");

        if (scriptsFacade.isFirstRun()) {
            scriptsFacade.createAllFromDirectory();
            logger.info("It's your first run of scmdb. Scmdb was initialized.");
            return;
        }

        checkUpdatedScripts();

        checkDeletedScripts();

        checkNewScripts();

        logger.info("Your database is up-to-date");
    }

    private void checkNewScripts() {
        List<SqlScript> newScripts = scriptsFacade.getNewScripts();
        List<SqlScript> newCommitScripts = newScripts.stream()
                                                     .sorted()
                                                     .filter(script -> script.getType() == ScriptType.COMMIT)
                                                     .collect(Collectors.toList());

        List<SqlScript> newRollbackScripts = newScripts.stream()
                                                       .filter(script -> script.getType() == ROLLBACK)
                                                       .collect(Collectors.toList());
        scriptsFacade.batchCreate(newRollbackScripts);

        if (appArguments.isExecuteScripts()) {
            logger.info("Executing scripts in your database:");
            for (SqlScript script : newCommitScripts) {
                executeScript(script);
                scriptsFacade.create(script);
                if (script.getStatus() == ScriptStatus.EXECUTED_WITH_ERRORS) {
                    System.exit(0);
                }
            }
        } else {
            logger.info("You should execute following script files to update your database:");
            scriptsFacade.copyScriptsToExecDir(newCommitScripts);
            newCommitScripts.forEach(script -> logger.info(script.getFile().getAbsolutePath()));
            scriptsFacade.batchCreate(newCommitScripts);
        }
    }

    private void checkDeletedScripts() {
        Map<String, SqlScript> deletedScripts = scriptsFacade.getDeletedScriptsMap();
        List<SqlScript> rollbacksToExec = deletedScripts.values().stream()
                                                        .filter(script -> script.getType() == ROLLBACK)
                                                        .filter(script -> deletedScripts.containsKey(script.getCommitName()))
                                                        .sorted(Comparator.reverseOrder())
                                                        .collect(Collectors.toList());
        if (rollbacksToExec.isEmpty()) {
            scriptsFacade.deleteAll(deletedScripts.values());
            return;
        }

        boolean executeRollbacks = false;

        if (appArguments.isExecuteScripts()) {
            logger.info("Do you really want to execute {} rollbacks? ", rollbacksToExec.size());
            logger.info("Type NO and rollbacks will be copied to EXECUTE_ME directory and marked as executed. Execute them manually and run scmdb again to execute new scripts.");
            logger.info("Type YES to continue and execute all rollbacks");
            executeRollbacks = userGrantsPermission();
        }

        if (executeRollbacks) {
            executeRollbacks(deletedScripts, rollbacksToExec);
            scriptsFacade.deleteAll(deletedScripts.values());
        } else {
            logger.info("You should execute following rollbacks to revert changes of deleted scripts. Execute them manually and run scmdb again to execute new scripts.");
            scriptsFacade.copyRollbacksToExecDir(rollbacksToExec);
            rollbacksToExec.forEach(script -> logger.info(script.getFile().getAbsolutePath()));
            scriptsFacade.deleteAll(deletedScripts.values());
            System.exit(0);
        }
    }

    private void executeRollbacks(Map<String, SqlScript> deletedScripts, List<SqlScript> rollbacksToExec) {
        for (SqlScript rollback : rollbacksToExec) {
            if (deletedScripts.containsKey(rollback.getCommitName())) {
                scriptsFacade.copyRollbackToExecDir(rollback);

                executeScript(rollback);
                //TODO: delete script after exec

                SqlScript commit = deletedScripts.get(rollback.getCommitName());
                scriptsFacade.delete(rollback.getId());
                scriptsFacade.delete(commit.getId());
                deletedScripts.keySet().remove(rollback.getName());
                deletedScripts.keySet().remove(rollback.getCommitName());

                if (rollback.getStatus() == ScriptStatus.EXECUTED_WITH_ERRORS) {
                    System.exit(0);
                }
            }
        }
    }

    private void checkUpdatedScripts() {
        List<SqlScript> updatedScripts = scriptsFacade.getUpdatedScripts();
        scriptsFacade.batchUpdate(updatedScripts);
        updatedScripts.forEach(script -> logger.warn("Script file [{}] was changed", script.getName()));
    }

    private boolean userGrantsPermission() {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            String str = br.readLine();
            if ("YES".equalsIgnoreCase(str)) {
                return true;
            } else if ("NO".equalsIgnoreCase(str)) {
                return false;
            } else {
                logger.info("Type yes or no");
                return userGrantsPermission();
            }

        } catch (IOException e) {
            throw new RuntimeException("Can't read user console input.", e);
        }
    }

    private void executeScript(SqlScript script) {
        logger.info("Executing script: [" + script.getName() + "]");
        int exitCode = scriptExecutor.execute(script);
        if (exitCode == 0) {
            script.setStatus(ScriptStatus.EXECUTED);
        } else {
            script.setStatus(ScriptStatus.EXECUTED_WITH_ERRORS);
        }
        if (exitCode != 0) {
            logger.error(SCRIPT_EXECUTION_ERROR_MESSAGE, script.getName());
        }
    }

    public void generateDdl() {
        List<SqlScript> newScripts = scriptsFacade.getNewScripts();
        List<SqlScript> newCommitScripts = newScripts.stream()
                                                     .sorted()
                                                     .filter(script -> script.getType() == ScriptType.COMMIT)
                                                     .filter(script -> !script.isUserSchemaScript())
                                                     .collect(Collectors.toList());
        ddlFacade.generateDdl(newCommitScripts);
    }
}
