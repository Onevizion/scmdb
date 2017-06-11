package com.onevizion.scmdb;

import com.onevizion.scmdb.facade.DbScriptFacade;
import com.onevizion.scmdb.facade.DdlFacade;
import com.onevizion.scmdb.vo.ScriptStatus;
import com.onevizion.scmdb.vo.ScriptType;
import com.onevizion.scmdb.vo.SqlScript;

import javax.annotation.Resource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.onevizion.scmdb.ColorLogger.Color.CYAN;
import static com.onevizion.scmdb.ColorLogger.Color.GREEN;
import static com.onevizion.scmdb.vo.ScriptType.ROLLBACK;

public class DbManager {
    private static final String SCRIPT_EXECUTION_ERROR_MESSAGE = "Fix and execute manually script [{}] and then run scmdb again to execute other scripts.";
    private static final String CANT_RUN_SQL_ERROR_MESSAGE = "Oracle SQLcl executable is not found. Please download it and make sure bin/sql is in your path";

    @Resource
    private DbScriptFacade scriptsFacade;

    @Resource
    private DdlFacade ddlFacade;

    @Resource
    private AppArguments appArguments;

    @Resource
    private SqlScriptExecutor scriptExecutor;

    @Resource
    private ColorLogger logger;

    public void updateDb() {
        logger.info("SCMDB {}", getClass().getPackage().getImplementationVersion());
        scriptExecutor.printVersion();
        logger.info("SCMDB\n");

        if (!checkAndCreateDbScriptTable()) {
            logger.info("Can't create DB objects used by SCMDB:");
            logger.info("Please execute script \"src/main/resources/create.sql\" manually");
        } else if (scriptsFacade.isFirstRun()) {
            scriptsFacade.createAllFromDirectory();
            logger.info("It's your first run of SCMDB. SCMDB was initialized.");
        } else {
            scriptsFacade.cleanExecDir();
            checkUpdatedScripts();
            checkDeletedScripts();
            checkNewScripts();
        }
        logger.info("\nSCMDB complete");
    }

    private void checkNewScripts() {
        List<SqlScript> newScripts = scriptsFacade.getNewScripts();
        if (newScripts.isEmpty()) {
            logger.info("No scripts to execute");
            return;
        }

        List<SqlScript> newCommitScripts = newScripts.stream()
                                                     .sorted()
                                                     .filter(script -> script.getType() == ScriptType.COMMIT)
                                                     .collect(Collectors.toList());

        List<SqlScript> newRollbackScripts = newScripts.stream()
                                                       .filter(script -> script.getType() == ROLLBACK)
                                                       .collect(Collectors.toList());
        scriptsFacade.batchCreate(newRollbackScripts);

        if (appArguments.isExecuteScripts()) {
            logger.info("Scripts to be executed:");
            newCommitScripts.forEach(script -> logger.info(script.getFile().getAbsolutePath()));
            for (SqlScript script : newCommitScripts) {
                executeScript(script);
                if (script.getStatus() != ScriptStatus.COMMAND_EXEC_FAILURE) {
                    scriptsFacade.create(script);
                }

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
            logger.info("Do you really want to execute {} rollbacks? ", GREEN, rollbacksToExec.size());
            logger.info("Type [no] and rollbacks will be copied to EXECUTE_ME directory and marked as executed. Execute them manually and run scmdb again to execute new scripts.", GREEN);
            logger.info("Type [yes] to continue and execute all rollbacks", GREEN);
            executeRollbacks = userGrantsPermission();
        }

        if (executeRollbacks) {
            executeRollbacks(deletedScripts, rollbacksToExec);
            scriptsFacade.deleteAll(deletedScripts.values());
        } else {
            logger.info("At first you should execute following rollbacks to revert changes of deleted scripts:");
            scriptsFacade.copyRollbacksToExecDir(rollbacksToExec);
            rollbacksToExec.forEach(script -> logger.info(script.getFile().getAbsolutePath(), GREEN));
            scriptsFacade.deleteAll(deletedScripts.values());

            if (appArguments.isExecuteScripts()) {
                System.exit(0);
            }
        }
    }

    private void executeRollbacks(Map<String, SqlScript> deletedScripts, List<SqlScript> rollbacksToExec) {
        for (SqlScript rollback : rollbacksToExec) {
            if (deletedScripts.containsKey(rollback.getCommitName())) {
                scriptsFacade.copyRollbackToExecDir(rollback);

                executeScript(rollback);

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
        updatedScripts.forEach(script -> logger.info("Script file [{}] was changed", CYAN, script.getName()));
    }

    private boolean userGrantsPermission() {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            String str = br.readLine();
            if ("yes".equalsIgnoreCase(str)) {
                return true;
            } else if ("no".equalsIgnoreCase(str)) {
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
        logger.info("\nExecuting script: [{}]", GREEN, script.getName());
        int exitCode = scriptExecutor.execute(script);
        if (exitCode == 0) {
            script.setStatus(ScriptStatus.EXECUTED);
        } else if (exitCode == 2) {
            script.setStatus(ScriptStatus.COMMAND_EXEC_FAILURE);
        } else {
            script.setStatus(ScriptStatus.EXECUTED_WITH_ERRORS);
        }

        if (exitCode == 2) {
            logger.error(CANT_RUN_SQL_ERROR_MESSAGE);
        } else if (exitCode != 0) {
            logger.error(SCRIPT_EXECUTION_ERROR_MESSAGE, script.getName());
        }
    }

    public void generateDdl() {
        logger.info("Extracting DDL for new and updated scripts");
        List<SqlScript> scripts = scriptsFacade.getNewScripts();
        scripts.addAll(scriptsFacade.getUpdatedScripts());
        List<SqlScript> scriptsToGenDdl = scripts.stream()
                                                 .sorted()
                                                 .filter(script -> script.getType() == ScriptType.COMMIT)
                                                 .filter(script -> !script.isUserSchemaScript())
                                                 .collect(Collectors.toList());
        ddlFacade.generateDdl(scriptsToGenDdl);
    }

    private boolean checkAndCreateDbScriptTable() {
        return scriptsFacade.isScriptTableExist() || scriptExecutor.createDbScriptTable();
    }
}
