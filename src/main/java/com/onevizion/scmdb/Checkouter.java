package com.onevizion.scmdb;

import com.onevizion.scmdb.dao.DbScriptDaoOra;
import com.onevizion.scmdb.facade.CheckoutFacade;
import com.onevizion.scmdb.vo.DbScriptVo;
import oracle.jdbc.pool.OracleDataSource;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Checkouter {

    private static final String DB_CNN_PROPS_ERROR_MESSAGE = "You should specify db connection properties using following format:"
        + " <username>/<password>@<host>:<port>:<SID>";

    private static final String JDBC_THIN_URL_PREFIX = "jdbc:oracle:thin:@";

    private static final String DB_SCRIPT_DIR_ERROR_MESSAGE = "You should specify absolute path to db scripts";

    private static final Logger logger = LoggerFactory.getLogger(Checkouter.class);

    public static void main(String[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException(DB_CNN_PROPS_ERROR_MESSAGE + "\n" + DB_SCRIPT_DIR_ERROR_MESSAGE);
        }
        String[] cnnProps = parseDbCnnStr(args[0]);
        File scriptDir = parseDbScriptDir(args[1]);
        boolean isGenDdl = false;
        if (args.length > 2) {
            isGenDdl = parseDbGenDdl(args[2]);
        }

        logger.debug("Initialize spring beans");
        ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath:beans.xml");

        OracleDataSource ds = (OracleDataSource) ctx.getBean("dataSource");
        ds.setUser(cnnProps[0]);
        ds.setPassword(cnnProps[1]);
        ds.setURL(cnnProps[2]);

        DbScriptDaoOra dbScriptDaoOra = ctx.getBean(DbScriptDaoOra.class);
        CheckoutFacade checkoutFacade = ctx.getBean(CheckoutFacade.class);

        logger.info("Checking out your database");
        logger.debug("Getting all scripts from db");
        Map<String, DbScriptVo> dbScripts = dbScriptDaoOra.readAll();
        if (dbScripts.isEmpty()) {
            logger.debug("Saving all scripts in db");
            checkoutFacade.createAllFromPath(scriptDir);
        } else {
            List<File> files = checkoutFacade.checkoutDbFromPath(scriptDir, dbScripts, isGenDdl);
            if (!files.isEmpty()) {
                logger.info("You should execute following script files to checkout your database:");
                for (File f : files) {
                    logger.info(f.getAbsolutePath());
                }
            }
        }
        logger.info("Your database is up-to-date");
    }

    private static boolean parseDbGenDdl(String arg) {
        if (NumberUtils.isNumber(arg)) {
            return NumberUtils.toInt(arg) == 1;
        } else {
            throw new IllegalArgumentException("Cannot parse generate ddl param using parameter: " + arg);
        }
    }

    private static File parseDbScriptDir(String path) {
        File f = new File(path);
        if (f.exists()) {
            return f;
        } else {
            throw new IllegalArgumentException("File or directory doesn't exist: " + path);
        }
    }

    private static String[] parseDbCnnStr(String cnnStr) {
        Pattern p = Pattern.compile("(.+?)/(.+?)@(.+)");
        Matcher m = p.matcher(cnnStr);
        String[] props = new String[3];
        if (m.matches() && m.groupCount() == 3) {
            props[0] = m.group(1);
            props[1] = m.group(2);
            props[2] = JDBC_THIN_URL_PREFIX + m.group(3);
        } else {
            throw new IllegalArgumentException(DB_CNN_PROPS_ERROR_MESSAGE);
        }
        return props;
    }
}