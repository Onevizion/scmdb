package com.onevizion.scmdb;

import com.onevizion.scmdb.dao.SqlScriptDaoOra;
import com.onevizion.scmdb.facade.CheckoutFacade;
import com.onevizion.scmdb.vo.DbCnnCredentials;
import com.onevizion.scmdb.vo.SqlScript;
import oracle.jdbc.pool.OracleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import static com.onevizion.scmdb.vo.ScriptStatus.EXECUTED;
import static com.onevizion.scmdb.vo.ScriptStatus.EXECUTED_WITH_ERRORS;
import static com.onevizion.scmdb.vo.ScriptType.COMMIT;
import static com.onevizion.scmdb.vo.ScriptType.ROLLBACK;

public class Checkouter {
    private static final Logger logger = LoggerFactory.getLogger(Checkouter.class);
    private static final String SCRIPT_EXECUTION_ERROR_MESSAGE = "Fix and execute manually script [{}] and then run scmdb again to execute other scripts.";

    private SqlScriptDaoOra dbScriptDaoOra;
    private CheckoutFacade checkoutFacade;
    private SqlScriptExecutor scriptExecutor;
    private File scriptDir;
    private boolean isGenDdl;
    private boolean isExecScripts;

    public Checkouter(DbCnnCredentials cnnCredentials, File scriptDir, boolean isGenDdl, boolean isExecScripts) {
        this.scriptDir = scriptDir;
        this.isGenDdl = isGenDdl;
        this.isExecScripts = isExecScripts;
        scriptExecutor = new SqlScriptExecutor(cnnCredentials);

        initSpring(cnnCredentials);
    }

    public void checkout() {
        logger.info("Checking out your database");
        logger.debug("Getting all scripts from db");

        ///region First run
        if (checkoutFacade.isFirstRun()) {
            logger.debug("Saving all scripts in db");
            checkoutFacade.createAllFromPath(scriptDir);
            return;
        }
        ///endregion

        //3 state: deleted, new, updated

        // new scripts
        //get all new scripts including rollbacks sort by script number exclude dev scripts
        List<SqlScript> newScripts = checkoutFacade.getNewScripts(scriptDir);
        List<SqlScript> newCommitScripts = newScripts.stream()
                                                     .sorted()
                                                     .filter(script -> script.getType() == COMMIT)
                                                     .collect(Collectors.toList());

        //commit - execute and save to db, rollback - just save to db

        //updated scripts
        //write list to console

        //deleted
        // in reverse order, 1 cycle - find commits, remove from db, find it rollback - exec if not exists, delete rollback from db 2 remove all other rollbacks

        //region Gen DDL
        if (isGenDdl) {
            checkoutFacade.genDdl(scriptDir, newCommitScripts);
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
            logger.info("You should execute following script files to checkout your database:");
            newCommitScripts.forEach(script -> logger.info(script.getFile().getAbsolutePath()));
            dbScriptDaoOra.batchCreate(newCommitScripts);
        }

        logger.info("Your database is up-to-date");
    }

    private void executeScripts(SqlScript script) {
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
    }

    private void initSpring(DbCnnCredentials cnnCredentials) {
        logger.debug("Initialize spring beans");
        ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:beans.xml");

        OracleDataSource ds = (OracleDataSource) ctx.getBean("dataSource");
        ds.setUser(cnnCredentials.getUser());
        ds.setPassword(cnnCredentials.getPassword());
        ds.setURL(cnnCredentials.getOracleUrl());

        dbScriptDaoOra = ctx.getBean(SqlScriptDaoOra.class);
        checkoutFacade = ctx.getBean(CheckoutFacade.class);
    }
}
