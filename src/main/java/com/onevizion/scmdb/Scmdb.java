package com.onevizion.scmdb;

import com.onevizion.scmdb.facade.DbScriptFacade;
import oracle.jdbc.pool.OracleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Scmdb {
    private static final Logger logger = LoggerFactory.getLogger(Scmdb.class);

    public static void main(String[] args) {
        logger.debug("Initialize spring beans");
        ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:beans.xml");

        AppArguments appArguments = ctx.getBean(AppArguments.class);
        appArguments.parse(args);

        DbScriptFacade sqlScriptsFacade = ctx.getBean(DbScriptFacade.class);
        sqlScriptsFacade.init();

        OracleDataSource ds = (OracleDataSource) ctx.getBean("dataSource");
        ds.setUser(appArguments.getOwnerCredentials().getUser());
        ds.setPassword(appArguments.getOwnerCredentials().getPassword());
        ds.setURL(appArguments.getOwnerCredentials().getOracleUrl());

        DbManager dbManager = ctx.getBean(DbManager.class);
        if (appArguments.isGenDdl()) {
            dbManager.generateDdl();
        } else {
            dbManager.updateDb();
        }
    }
}