package com.onevizion.scmdb;

import com.onevizion.scmdb.facade.DdlFacade;
import com.onevizion.scmdb.facade.SqlScriptsFacade;
import com.onevizion.scmdb.vo.ScriptType;
import com.onevizion.scmdb.vo.SqlScript;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

public class Checkouter {
    private static final Logger logger = LoggerFactory.getLogger(Checkouter.class);
    private static final String SCRIPT_EXECUTION_ERROR_MESSAGE = "Fix and execute manually script [{}] and then run scmdb again to execute other scripts.";

    @Resource
    private SqlScriptsFacade scriptsFacade;

    @Resource
    private DdlFacade ddlFacade;

    @Resource
    private AppArguments appArguments;

    public void updateDb() {
        logger.info("Checking out your database");
        logger.debug("Getting all scripts from db");

        ///region First run
        if (scriptsFacade.isFirstRun()) {
            logger.debug("Saving all scripts in db");
            scriptsFacade.createAll();
            return;
        }
        ///endregion

        //3 state: deleted, new, updated

        //updated scripts
        //write list to console
        List<SqlScript> updatedScripts = scriptsFacade.getUpdatedScripts();
        scriptsFacade.batchUpdate(updatedScripts);


        // new scripts
        //get all new scripts including rollbacks sort by script number exclude dev scripts
        /*List<SqlScript> newScripts = scriptsFacade.getNewScripts(scriptDir);
        List<SqlScript> newCommitScripts = newScripts.stream()
                                                     .sorted()
                                                     .filter(script -> script.getType() == COMMIT)
                                                     .collect(Collectors.toList());

        //commit - execute and save to db, rollback - just save to db



        //deleted
        // in reverse order, 1 cycle - find commits, remove from db, find it rollback - exec if not exists, delete rollback from db 2 remove all other rollbacks

        //region Gen DDL
        if (isGenDdl) {
            scriptsFacade.generateDdl(scriptDir, newCommitScripts);
            return;
        }
        //endregion
        List<SqlScript> newRollbackScripts = newScripts.stream()
                                                       .filter(script -> script.getType() == ROLLBACK)
                                                       .collect(Collectors.toList());
        dbScriptDaoOra.batchCreate(newRollbackScripts);

        if (isExecScripts) {
            logger.info("Executing scripts in your database:");
            for (SqlScript script : newCommitScripts) {
                executeScripts(script);
                dbScriptDaoOra.create(script);
                if (script.getStatus() == EXECUTED_WITH_ERRORS) {
                    return;
                }
            }
        } else {
            logger.info("You should execute following script files to updateDb your database:");
            scriptsFacade.copyScriptsToExecDir(scriptDir, newCommitScripts, Collections.emptyList());
            newCommitScripts.forEach(script -> logger.info(script.getFile().getAbsolutePath()));
            dbScriptDaoOra.batchCreate(newCommitScripts);
        }
*/
        logger.info("Your database is up-to-date");
    }

    public void generateDdl() {
        List<SqlScript> newScripts = scriptsFacade.getNewScripts();
        List<SqlScript> newCommitScripts = newScripts.stream()
                                                     .sorted()
                                                     .filter(script -> script.getType() == ScriptType.COMMIT)
                                                     .collect(Collectors.toList());
        ddlFacade.generateDdl(newCommitScripts);
    }

   /* private void executeScripts(SqlScript script) {
        logger.info("Executing script: [" + script.getName() + "]");
        int exitCode = scriptExecutor.execute(script.getFile());
        if (exitCode == 0) {
            script.setStatus(EXECUTED);
        } else {
            script.setStatus(EXECUTED_WITH_ERRORS);
        }
        if (exitCode != 0) {
            logger.error(SCRIPT_EXECUTION_ERROR_MESSAGE, script.getFile().getName());
        }
    }*/
}
