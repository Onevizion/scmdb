package com.onevizion.scmdb;

import com.onevizion.scmdb.exception.ScriptExecException;
import com.onevizion.scmdb.vo.DbCnnCredentials;
import com.onevizion.scmdb.vo.SchemaType;
import com.onevizion.scmdb.vo.SqlScript;
import joptsimple.internal.Strings;
import oracle.dbtools.raptor.newscriptrunner.ScriptExecutor;
import oracle.dbtools.raptor.newscriptrunner.ScriptRunnerContext;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;

import static com.onevizion.scmdb.ColorLogger.Color.GREEN;
import static com.onevizion.scmdb.ColorLogger.Color.YELLOW;
import static com.onevizion.scmdb.Scmdb.EXIT_CODE_SUCCESS;
import static com.onevizion.scmdb.vo.SchemaType.OWNER;
import static com.onevizion.scmdb.vo.ScriptType.COMMIT;
import static java.time.format.DateTimeFormatter.ISO_TIME;
import static oracle.dbtools.raptor.newscriptrunner.ScriptRunnerContext.ERR_ENCOUNTERED;
import static org.apache.commons.lang3.time.DurationFormatUtils.formatDurationHMS;

public class SqlScriptExecutor {
    private static final String SQL_CLIENT_COMMAND = "sql";
    private static final String SQL_COMMAND = "@%s %s";

    private static final String INVALID_OBJECT_PREFIX = "Invalid objects in";
    private static final String INVALID_OBJECT_REGEX = "^(\\w+\\s){0,2}\\w+\\s+\\S+\\s+is invalid.\\s*";
    private static final String ERROR_STARTING_AT_LINE = "Error starting at line :";
    private static final String CREATE_SQL = "create.sql";
    private static final int SCRIPT_EXIT_CODE_ERROR = 1;
    private static final int SCRIPT_EXIT_CODE_SUCCESS = 0;
    private static final String ERROR_LAST_LINE_START = "*Action:";

    private boolean isErrorMsgStarted = false;

    @Autowired
    private AppArguments appArguments;

    @Autowired
    private ColorLogger logger;

    public SqlScriptExecutor() {
    }

    public int execute(SqlScript script) {
        DbCnnCredentials cnnCredentials = appArguments.getDbCredentials(script.getSchemaType());
        logger.info("\nExecuting script [{}] in schema [{}]. Start: {}", GREEN, script.getName(),
                cnnCredentials.getSchemaWithUrlBeforeDot(), ZonedDateTime.now().format(ISO_TIME));

        File workingDir = script.getFile().getParentFile();
        File wrapperScriptFile = getTmpWrapperScript(script.getSchemaType(), workingDir);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             BufferedOutputStream bos = new BufferedOutputStream(baos);
             Connection connection = DriverManager.getConnection(cnnCredentials.getOracleUrl(),
                                                                 cnnCredentials.getSchemaName(),
                                                                 cnnCredentials.getPassword())) {
            connection.setAutoCommit(false);
            ScriptExecutor executor = new ScriptExecutor(connection);
            ScriptRunnerContext ctx = new ScriptRunnerContext();

            ctx.setBaseConnection(connection);
            ctx.setOutputStreamWrapper(bos);
            ctx.getOutputStream().setRemoveForcePrint(true);

            executor.setScriptRunnerContext(ctx);
            executor.setOut(bos);
            executor.setStmt(String.format(SQL_COMMAND, wrapperScriptFile.getAbsolutePath(), script.getFile().getAbsolutePath()));

            Instant start = Instant.now();
            executor.run();
            String scriptExecutionTime = formatDurationHMS(Duration.between(start, Instant.now()).toMillis());

            printFormattedScriptResult(baos.toString(StandardCharsets.UTF_8));
            logger.info("\n[{}] runtime: {}", GREEN, script.getName(), scriptExecutionTime);

            return (boolean) ctx.getProperty(ERR_ENCOUNTERED) ? SCRIPT_EXIT_CODE_ERROR : SCRIPT_EXIT_CODE_SUCCESS;
        } catch (IOException e ) {
            logger.error("Error.", e);
            return SCRIPT_EXIT_CODE_ERROR;
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

    private void printFormattedScriptResult(String results) {
        String[] lines = results.split("\n");

        for (String line : lines) {
            if (line.startsWith(INVALID_OBJECT_PREFIX)) {
                logger.warn(line, YELLOW);
            } else if (line.matches(INVALID_OBJECT_REGEX)) {
                logger.warn(line, YELLOW);
            } else if (line.startsWith(ERROR_STARTING_AT_LINE)) {
                isErrorMsgStarted = true;
                logger.error(line);
            } else if (line.startsWith(ERROR_LAST_LINE_START) || Strings.isNullOrEmpty(line)) {
                logger.error(line);
                isErrorMsgStarted = false;
            } else if (isErrorMsgStarted) {
                logger.error(line);
            } else {
                logger.info(line);
            }
        }
    }
}
