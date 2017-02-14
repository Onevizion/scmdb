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
import java.util.Collections;
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
                executeScripts(script);
                scriptsFacade.create(script);
                if (script.getStatus() == ScriptStatus.EXECUTED_WITH_ERRORS) {
                    return;
                }
            }
        } else {
            logger.info("You should execute following script files to updateDb your database:");
            scriptsFacade.copyRollbacksToExecDir(newCommitScripts, Collections.emptyList());
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
        deletedScripts.values().removeAll(rollbacksToExec);
        scriptsFacade.deleteAll(deletedScripts.values());

        if (appArguments.isExecuteScripts()) {
            logger.info("Do you really wan't to execute {} rollbacks? ", rollbacksToExec.size());
            logger.info("Type NO and rollbacks will be copied to EXECUTE_ME directory and marked as executed. Execute them manually and run scmdb again to execute new scripts.");
            logger.info("Type YES to continue and execute all rollbacks");

            if (askUserPermission()) {
                //executeScripts(rollbacksToExec);
            } else {
                scriptsFacade.copyRollbacksToExecDir(rollbacksToExec, Collections.emptyList());
            }
        } else {
            logger.info("You should execute following script files to updateDb your database:");
            scriptsFacade.copyRollbacksToExecDir(rollbacksToExec, Collections.emptyList());
            rollbacksToExec.forEach(script -> logger.info(script.getFile().getAbsolutePath()));
            scriptsFacade.batchCreate(rollbacksToExec);
        }
    }

    private void checkUpdatedScripts() {
        List<SqlScript> updatedScripts = scriptsFacade.getUpdatedScripts();
        scriptsFacade.batchUpdate(updatedScripts);
        updatedScripts.forEach(script -> logger.warn("Script file [{}] was changed", script.getName()));
    }

    private boolean askUserPermission() {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        try {
            String str = br.readLine();
            if ("YES".equalsIgnoreCase(str)) {
                return true;
            } else if ("NO".equalsIgnoreCase(str)) {
                return false;
            } else {
                logger.info("Type yes or no");
                return askUserPermission();
            }

        } catch (IOException e) {
            throw new RuntimeException("Can't read user console input.", e);
        }
    }

    private void executeScripts(SqlScript script) {
        logger.info("Executing script: [" + script.getName() + "]");
        int exitCode = scriptExecutor.execute(script.getFile());
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
                                                     .collect(Collectors.toList());
        ddlFacade.generateDdl(newCommitScripts);
    }
}
