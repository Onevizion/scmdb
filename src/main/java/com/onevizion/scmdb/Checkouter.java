package com.onevizion.scmdb;

import com.onevizion.scmdb.dao.DbScriptDaoOra;
import com.onevizion.scmdb.facade.CheckoutFacade;
import com.onevizion.scmdb.vo.DbCnnCredentials;
import com.onevizion.scmdb.vo.DbScriptStatus;
import com.onevizion.scmdb.vo.DbScriptVo;
import oracle.jdbc.pool.OracleDataSource;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class Checkouter {
    private static final Logger logger = LoggerFactory.getLogger(Checkouter.class);
    public static final String SCRIPT_EXECUTION_ERROR_MESSAGE = "Fix and execute manually script [{}] and then run scmdb again to execute other scripts.";
    private DbScriptDaoOra dbScriptDaoOra;
    private CheckoutFacade checkoutFacade;
    private DbCnnCredentials cnnCredentials;
    private File scriptDir;
    private boolean isGenDdl;
    private boolean isExecScripts;

    public Checkouter(DbCnnCredentials cnnCredentials, File scriptDir, boolean isGenDdl, boolean isExecScripts) {
        this.cnnCredentials = cnnCredentials;
        this.scriptDir = scriptDir;
        this.isGenDdl = isGenDdl;
        this.isExecScripts = isExecScripts;

        initSpring();
    }

    public void checkout() {
        logger.info("Checking out your database");
        logger.debug("Getting all scripts from db");
        if (checkoutFacade.isFirstRun()) {
            logger.debug("Saving all scripts in db");
            checkoutFacade.createAllFromPath(scriptDir);
            return;
        }

        Collection<DbScriptVo> scriptsToExec = checkoutFacade.getScriptsToExec(scriptDir);
        if (isGenDdl) {
            checkoutFacade.genDdl(scriptDir, scriptsToExec);
            return;
        }

        skipDevScripts(scriptsToExec);
        if (!scriptsToExec.isEmpty()) {
            if (isExecScripts) {
                SqlScriptExecutor scriptExecutor = new SqlScriptExecutor(cnnCredentials);
                logger.info("Executing scripts in your database:");
                for (DbScriptVo script : scriptsToExec) {
                    logger.info("Executing script: [" + script.getName() + "]");
                    int exitCode = scriptExecutor.execute(script.getFile());
                    if (exitCode == 0) {
                        script.setStatus(DbScriptStatus.EXECUTED);
                    } else {
                        script.setStatus(DbScriptStatus.EXECUTED_WITH_ERRORS);
                    }
                    dbScriptDaoOra.create(script);
                    if (exitCode != 0) {
                        logger.error(SCRIPT_EXECUTION_ERROR_MESSAGE, script.getFile().getName());
                        return;
                    }
                }
            } else {
                logger.info("You should execute following script files to checkout your database:");
                for (DbScriptVo script : scriptsToExec) {
                    logger.info(script.getFile().getAbsolutePath());
                }
                dbScriptDaoOra.batchCreate(scriptsToExec);
            }
        }
        logger.info("Your database is up-to-date");
    }

    private void skipDevScripts(Collection<DbScriptVo> scripts) {
        Set<DbScriptVo> ignoredScripts = new HashSet<DbScriptVo>();
        Iterator<DbScriptVo> iterator = scripts.iterator();
        while (iterator.hasNext()) {
            DbScriptVo scriptVo = iterator.next();
            if (isDevScript(scriptVo)) {
                ignoredScripts.add(scriptVo);
                iterator.remove();
            }
        }

        if (!ignoredScripts.isEmpty()) {
            for (DbScriptVo script : ignoredScripts) {
                logger.info("Script was ignored [" + script.getName() + "]");
            }
        }
    }

    private boolean isDevScript(DbScriptVo scriptVo) {
        String[] parts = scriptVo.getName().split("_");
        return parts.length <= 1 || !NumberUtils.isDigits(parts[0]);
    }

    private void initSpring() {
        logger.debug("Initialize spring beans");
        ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:beans.xml");

        OracleDataSource ds = (OracleDataSource) ctx.getBean("dataSource");
        ds.setUser(cnnCredentials.getUser());
        ds.setPassword(cnnCredentials.getPassword());
        ds.setURL(cnnCredentials.getOracleUrl());

        dbScriptDaoOra = ctx.getBean(DbScriptDaoOra.class);
        checkoutFacade = ctx.getBean(CheckoutFacade.class);
    }
}
