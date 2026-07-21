package com.onevizion.scmdb;

import com.onevizion.scmdb.exception.ScmdbException;
import com.onevizion.scmdb.facade.DbScriptFacade;
import oracle.dbtools.db.DBUtil;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.logging.Level;

import static com.onevizion.scmdb.vo.SchemaType.*;

public class Scmdb {

    public static final int EXIT_CODE_ERROR = 1;
    public static final int EXIT_CODE_SUCCESS = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger("STDOUT");

    public static void main(String[] args) {
        try {
            LOGGER.debug("Initialize spring beans");
            ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:beans.xml");

            String buildInformation = ctx.getBean("buildInformation", String.class);
            LOGGER.info("SCMDB Build Information: [{}]", buildInformation);

            AppArguments appArguments = ctx.getBean(AppArguments.class);
            appArguments.parse(args, !ResourceResolveUtils.containsClassPathScripts());

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
            if (appArguments.isBackport()) {
                BackportRunner backportRunner = ctx.getBean(BackportRunner.class);
                dbManager.runBackport(backportRunner);
            } else if (appArguments.isRollback()) {
                dbManager.runRollback();
            } else if (appArguments.isGenDdl()) {
                if (appArguments.isAll()) {
                    dbManager.generateDdlForAllObjects();
                } else {
                    dbManager.generateDdlForNewOrChangedScripts();
                }
            } else {
                dbManager.updateDb();
            }
        } catch (ScmdbException e) {
            LOGGER.error(e.getMessage());
            System.exit(EXIT_CODE_ERROR);
        } catch (Exception e) {
            LOGGER.error("Scmdb internal error", e);
            System.exit(EXIT_CODE_ERROR);
        }
        LOGGER.info("\nSCMDB complete");
        System.exit(EXIT_CODE_SUCCESS);
    }
}