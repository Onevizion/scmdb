package com.onevizion.scmdb;

import com.onevizion.scmdb.exception.ScriptExecException;
import com.onevizion.scmdb.vo.DbCnnCredentials;
import com.onevizion.scmdb.vo.SchemaType;
import com.onevizion.scmdb.vo.SqlScript;
import oracle.dbtools.db.DBUtil;
import oracle.dbtools.raptor.newscriptrunner.ScriptExecutor;
import oracle.dbtools.raptor.newscriptrunner.ScriptRunnerContext;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.onevizion.scmdb.ColorLogger.Color.GREEN;
import static com.onevizion.scmdb.ColorLogger.Color.YELLOW;
import static com.onevizion.scmdb.Scmdb.EXIT_CODE_SUCCESS;
import static com.onevizion.scmdb.vo.SchemaType.OWNER;
import static com.onevizion.scmdb.vo.ScriptType.COMMIT;
import static java.time.format.DateTimeFormatter.ISO_TIME;
import static oracle.dbtools.raptor.newscriptrunner.ScriptRunnerContext.ERR_ENCOUNTERED;
import static org.apache.commons.lang3.time.DurationFormatUtils.formatDurationHMS;

public class SqlScriptExecutor {
    private static final String SQL_COMMAND = "@%s %s";
    private static final String CREATE_SQL = "create.sql";
    private static final int SCRIPT_EXIT_CODE_ERROR = 1;
    private static final int SCRIPT_EXIT_CODE_SUCCESS = 0;

    @Autowired
    private AppArguments appArguments;

    @Autowired
    private ColorLogger logger;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private DataSource userDataSource;

    @Autowired
    private DataSource rptDataSource;

    @Autowired
    private DataSource pkgDataSource;

    public int execute(SqlScript script) {
        DbCnnCredentials cnnCredentials = appArguments.getDbCredentials(script.getSchemaType());
        logger.info("\nExecuting script [{}] in schema [{}]. Start: {}", GREEN, script.getName(),
                    cnnCredentials.getSchemaWithUrlBeforeDot(), ZonedDateTime.now().format(ISO_TIME));

        File workingDir = script.getFile().getParentFile();
        File wrapperScriptFile = getTmpWrapperScript(script.getSchemaType(), workingDir);

        try (Connection connection = getConnection(script.getSchemaType(), cnnCredentials.getSchemaName())) {
            connection.setAutoCommit(false);
            ScriptExecutor executor = new ScriptExecutor(connection);
            ScriptRunnerContext ctx = new ScriptRunnerContext();

            ctx.setBaseConnection(connection);
            executor.setScriptRunnerContext(ctx);
            executor.setStmt(String.format(SQL_COMMAND, wrapperScriptFile.getAbsolutePath(),
                                           script.getFile().getAbsolutePath()));

            Instant start = Instant.now();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
            PrintStream old = System.out;
            System.setOut(ps);
            executor.run();
            System.out.flush();
            System.setOut(old);
            script.setOutput(baos.toString());
            System.out.println(baos.toString());

            String scriptExecutionTime = formatDurationHMS(Duration.between(start, Instant.now()).toMillis());

            logger.info("\n[{}] runtime: {}", GREEN, script.getName(), scriptExecutionTime);

            return (boolean) ctx.getProperty(ERR_ENCOUNTERED) ? SCRIPT_EXIT_CODE_ERROR : SCRIPT_EXIT_CODE_SUCCESS;
        } catch (SQLException e) {
            logger.error("Error during connection DB.", e);
            return SCRIPT_EXIT_CODE_ERROR;
        } finally {
            wrapperScriptFile.delete();
        }
    }

    private File getTmpWrapperScript(SchemaType schemaType, File workingDir) {
        ClassLoader classLoader = getClass().getClassLoader();
        URL wrapperScript;
        if (schemaType.isCompileInvalids()) {
            if (appArguments.isIgnoreErrors()) {
                wrapperScript = classLoader.getResource("compile_invalids_wrapper_not_fail_on_error.sql");
            } else {
                wrapperScript = classLoader.getResource("compile_invalids_wrapper_fail_on_error.sql");
            }
        } else {
            if (appArguments.isIgnoreErrors()) {
                wrapperScript = classLoader.getResource("script_wrapper_not_fail_on_error.sql");
            } else {
                wrapperScript = classLoader.getResource("script_wrapper_fail_on_error.sql");
            }
        }

        File tmpFile = new File(workingDir.getAbsolutePath() + File.separator + "tmp.sql");

        try {
            FileUtils.copyURLToFile(wrapperScript, tmpFile);
        } catch (IOException e) {
            throw new RuntimeException("Can't copy tmp wrapper file.", e);
        }

        return tmpFile;
    }

    public void createDbScriptTable() {
        File scriptsDirectory = appArguments.getScriptsDirectory();
        String tmpFileName = new Date().getTime() + CREATE_SQL;
        String tmpFilePath = scriptsDirectory.getAbsolutePath() + File.separator + tmpFileName;
        File tmpFile = new File(tmpFilePath);
        URL resource = getClass().getClassLoader().getResource(CREATE_SQL);
        try {
            FileUtils.copyURLToFile(resource, tmpFile);
        } catch (IOException e) {
            throw new RuntimeException("Can't copy " + CREATE_SQL + " file.", e);
        }

        SqlScript sqlScript = new SqlScript();
        sqlScript.setName(tmpFileName);
        sqlScript.setFile(tmpFile);
        sqlScript.setType(COMMIT);
        sqlScript.setSchemaType(OWNER);

        int exitCode = execute(sqlScript);
        tmpFile.delete();
        if (exitCode != EXIT_CODE_SUCCESS) {
            logger.error("Please execute script \"src/main/resources/create.sql\" manually");
            throw new ScriptExecException("Can't create DB objects used by SCMDB.");
        }
    }

    private Connection getConnection(SchemaType schemaType, String schemaName) {
        try {
            switch (schemaType) {
                case USER: return userDataSource.getConnection();
                case RPT: return rptDataSource.getConnection();
                case PKG: return pkgDataSource.getConnection();
                default: return dataSource.getConnection();
            }
        } catch (SQLException exception) {
            throw new RuntimeException(MessageFormat.format("Error during connection to the schema [{}].", schemaName),
                                       exception);
        }
    }

}
