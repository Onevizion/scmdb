package com.onevizion.scmdb;

import ch.qos.logback.classic.Logger;
import com.onevizion.scmdb.exception.ScmdbException;
import com.onevizion.scmdb.facade.DbScriptFacade;
import oracle.dbtools.db.DBUtil;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceImpl;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.logging.Level;

import static com.onevizion.scmdb.vo.SchemaType.*;

public class Scmdb {
    private static final Logger logger = (Logger) LoggerFactory.getLogger("STDOUT");

    public static final int EXIT_CODE_ERROR = 1;
    public static final int EXIT_CODE_SUCCESS = 0;

    public static void main(String[] args) {
        try {
            logger.debug("Initialize spring beans");
            ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:beans.xml");

            AppArguments appArguments = ctx.getBean(AppArguments.class);
            appArguments.parse(args);

            DbScriptFacade sqlScriptsFacade = ctx.getBean(DbScriptFacade.class);
            sqlScriptsFacade.init();

            PoolDataSourceImpl ownerDs = (PoolDataSourceImpl) ctx.getBean("dataSource");
            appArguments.fillDataSourceCredentials(ownerDs, OWNER);

            PoolDataSource userDataSource = (PoolDataSource) ctx.getBean("userDataSource");
            appArguments.fillDataSourceCredentials(userDataSource, USER);

            PoolDataSource rptDataSource = (PoolDataSource) ctx.getBean("rptDataSource");
            appArguments.fillDataSourceCredentials(rptDataSource, RPT);

            PoolDataSource pkgDataSourceDs = (PoolDataSource) ctx.getBean("pkgDataSource");
            appArguments.fillDataSourceCredentials(pkgDataSourceDs, PKG);

            //Off logger for oracle.dbtools.db.Oracle Util, if not, Java exception gets into the sql log
            final java.util.logging.Logger dbUtilLogger = java.util.logging.Logger.getLogger(DBUtil.class.getName());
            dbUtilLogger.setLevel(Level.OFF);

            DbManager dbManager = ctx.getBean(DbManager.class);
            if (appArguments.isGenDdl()) {
                if (appArguments.isAll()) {
                    dbManager.generateDdlForAllObjects();
                } else {
                    dbManager.generateDdlForNewOrChangedScripts();
                }
            } else {
                dbManager.updateDb();
            }
        } catch (ScmdbException e) {
            logger.error(e.getMessage());
            System.exit(EXIT_CODE_ERROR);
        } catch (Exception e) {
            logger.error("Scmdb internal error", e);
            System.exit(EXIT_CODE_ERROR);
        }
        logger.info("\nSCMDB complete");
        System.exit(EXIT_CODE_SUCCESS);
    }
}