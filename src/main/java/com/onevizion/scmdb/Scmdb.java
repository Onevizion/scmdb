package com.onevizion.scmdb;

import com.onevizion.scmdb.facade.DbScriptFacade;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
import oracle.jdbc.pool.OracleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.env.JOptCommandLinePropertySource;
import org.springframework.core.env.PropertySource;

import java.io.File;

import static java.util.Arrays.asList;

public class Scmdb {
    private static final Logger logger = LoggerFactory.getLogger(Scmdb.class);

    public static void main(String[] args) {
        logger.debug("Initialize spring beans");
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:beans.xml");

        OptionParser parser = new OptionParser();
        parser.accepts("owner-schema").withRequiredArg().ofType(String.class).required();
        parser.accepts("user-schema").withRequiredArg().ofType(String.class);
        parser.accepts("scripts-dir").withRequiredArg().ofType(File.class).required();

        parser.acceptsAll(asList("e", "exec"));
        parser.acceptsAll(asList("d", "gen-ddl")).availableUnless("exec");
        parser.accepts("no-color");

        OptionSet options = parser.parse(args);
        PropertySource clps = new JOptCommandLinePropertySource(options);
        ctx.getEnvironment().getPropertySources().addFirst(clps);
        ctx.refresh();

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