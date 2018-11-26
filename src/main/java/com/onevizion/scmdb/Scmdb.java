package com.onevizion.scmdb;

import com.onevizion.scmdb.facade.DbScriptFacade;
import com.onevizion.scmdb.vo.DbCnnCredentials;
import oracle.ucp.jdbc.PoolDataSourceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.sql.SQLException;

import static com.onevizion.scmdb.vo.SchemaType.OWNER;

public class Scmdb {
    private static final Logger logger = LoggerFactory.getLogger(Scmdb.class);

    public static void main(String[] args) throws SQLException {
        logger.debug("Initialize spring beans");
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:beans.xml");

        AppArguments appArguments = ctx.getBean(AppArguments.class);
        appArguments.parse(args);

        DbScriptFacade sqlScriptsFacade = ctx.getBean(DbScriptFacade.class);
        sqlScriptsFacade.init();

        PoolDataSourceImpl ownerDs = (PoolDataSourceImpl) ctx.getBean("dataSource");
        DbCnnCredentials ownerCredentials = appArguments.getDbCredentials(OWNER);
        ownerDs.setUser(ownerCredentials.getSchemaName());
        ownerDs.setPassword(ownerCredentials.getPassword());
        ownerDs.setURL(ownerCredentials.getOracleUrl());

        DbManager dbManager = ctx.getBean(DbManager.class);
        if (appArguments.isGenDdl()) {
            if(appArguments.isAll()){
                dbManager.generateDdlForAllObjects();
            }else{
                dbManager.generateDdlForNewOrChangedScripts();
            }
        } else {
            dbManager.updateDb();
        }
    }
}