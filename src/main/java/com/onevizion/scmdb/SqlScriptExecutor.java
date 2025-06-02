package com.onevizion.scmdb;

import com.onevizion.scmdb.exception.DbConnectionException;
import com.onevizion.scmdb.exception.ScriptExecException;
import com.onevizion.scmdb.vo.DbCnnCredentials;
import com.onevizion.scmdb.vo.SchemaType;
import com.onevizion.scmdb.vo.SqlScript;
import oracle.dbtools.raptor.newscriptrunner.ScriptExecutor;
import oracle.dbtools.raptor.newscriptrunner.ScriptRunnerContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.TeeOutputStream;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;

import static com.onevizion.scmdb.ColorLogger.Color.GREEN;
import static com.onevizion.scmdb.Scmdb.EXIT_CODE_SUCCESS;
import static com.onevizion.scmdb.vo.SchemaType.OWNER;
import static com.onevizion.scmdb.vo.ScriptType.COMMIT;
import static java.time.format.DateTimeFormatter.ISO_TIME;
import static oracle.dbtools.raptor.newscriptrunner.ScriptRunnerContext.ERR_ENCOUNTERED;
import static org.apache.commons.lang3.time.DurationFormatUtils.formatDurationHMS;

public class SqlScriptExecutor {
    private static final String SQL_COMMAND = "@%s %s";
    private static final String CREATE_SQL = "create.sql";
    private static final String COMPILE_SCHEMAS_SQL = "compile_schemas.sql";
    private static final String SHOW_INVALID_OBJECTS_SQL = "check_invalid_objects.sql";
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

    private void executeResourceScript(String scriptFileName, String errorMessage) {
        executeResourceScript(scriptFileName, errorMessage, false);
    }

    public void showInvalidObjects() {
        try {
            executeResourceScript(SHOW_INVALID_OBJECTS_SQL, "_rpt, _pkg, _user schema invalid objects are not visible from the owner schema.", false);
        } catch (ScriptExecException e) {
            // Log with WARNING color but don't rethrow - we don't want to fail the application
            logger.warn("Unable to check invalid objects: {}", ColorLogger.Color.YELLOW, e.getMessage());
        }
    }

    public int execute(SqlScript script) {
        File wrapperScriptFile = getTmpWrapperScript(script.getSchemaType().isCompileInvalids(),
                                                     appArguments.isIgnoreErrors(),
                                                     script.getFile().getParentFile());
        return execute(script, wrapperScriptFile);
    }

    private int execute(SqlScript script, File wrapperScriptFile) {
        DbCnnCredentials cnnCredentials = appArguments.getDbCredentials(script.getSchemaType());
        logger.info("\nExecuting script [{}] in schema [{}]. Start: {}", GREEN, script.getName(),
                cnnCredentials.getSchemaWithUrlBeforeDot(), ZonedDateTime.now().format(ISO_TIME));

        try (Connection connection = getConnection(script.getSchemaType(), cnnCredentials.getSchemaName());
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            connection.setAutoCommit(false);
            ScriptExecutor executor = new ScriptExecutor(connection);
            ScriptRunnerContext ctx = new ScriptRunnerContext();

            ctx.setBaseConnection(connection);
            ctx.setOutputStreamWrapper(new BufferedOutputStream(new TeeOutputStream(System.out, outputStream)));
            executor.setScriptRunnerContext(ctx);
            executor.setStmt(String.format(SQL_COMMAND, wrapperScriptFile.getAbsolutePath(),
                                           script.getFile().getAbsolutePath()));

            Instant start = Instant.now();
            executor.run();
            script.setOutput(outputStream.toString());

            String scriptExecutionTime = formatDurationHMS(Duration.between(start, Instant.now()).toMillis());

            logger.info("\n[{}] runtime: {}", GREEN, script.getName(), scriptExecutionTime);

            return (boolean) ctx.getProperty(ERR_ENCOUNTERED) ? SCRIPT_EXIT_CODE_ERROR : SCRIPT_EXIT_CODE_SUCCESS;
        } catch (SQLException e) {
            logger.error("Error during connection DB.", e);
            return SCRIPT_EXIT_CODE_ERROR;

        } catch (IOException e) {
            logger.error("Can't close OutputStream", e);
            return SCRIPT_EXIT_CODE_ERROR;

        } finally {
            wrapperScriptFile.delete();
        }
    }

    private File getTmpWrapperScript(boolean compileInvalids, boolean ignoreErrors, File workingDir) {
        ClassLoader classLoader = getClass().getClassLoader();
        URL wrapperScript;
        if (compileInvalids) {
            if (ignoreErrors) {
                wrapperScript = classLoader.getResource("compile_invalids_wrapper_not_fail_on_error.sql");
            } else {
                wrapperScript = classLoader.getResource("compile_invalids_wrapper_fail_on_error.sql");
            }
        } else {
            if (ignoreErrors) {
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
        executeResourceScript(CREATE_SQL, "Can't create DB objects used by SCMDB.");
    }

    public void executeCompileSchemas() {
        executeResourceScript(COMPILE_SCHEMAS_SQL, "Can't compile invalid objects in _user, _rpt, _pkg schemas.");
    }

    private Connection getConnection(SchemaType schemaType, String schemaName) {
        try {
            return switch (schemaType) {
                case USER -> userDataSource.getConnection();
                case RPT -> rptDataSource.getConnection();
                case PKG -> pkgDataSource.getConnection();
                case OWNER -> dataSource.getConnection();
            };
        } catch (SQLException exception) {
            throw new DbConnectionException(
                    MessageFormat.format("Error during connection to the schema [{}].", schemaName),
                    exception);
        }
    }

    private void executeResourceScript(String scriptFileName, String errorMessage, boolean ignoreSqlLog) {
        File scriptsDirectory = appArguments.getScriptsDirectory();
        String tmpFileName = new Date().getTime() + scriptFileName;
        String tmpFilePath = scriptsDirectory.getAbsolutePath() + File.separator + tmpFileName;
        File tmpFile = new File(tmpFilePath);
        URL resource = getClass().getClassLoader().getResource(scriptFileName);

        try {
            FileUtils.copyURLToFile(resource, tmpFile);
        } catch (IOException e) {
            throw new RuntimeException("Can't copy " + scriptFileName + " file.", e);
        }

        SqlScript sqlScript = new SqlScript();
        sqlScript.setName(tmpFileName);
        sqlScript.setFile(tmpFile);
        sqlScript.setType(COMMIT);
        sqlScript.setSchemaType(OWNER);

        File wrapperScriptFile = getTmpWrapperScript(false, false, sqlScript.getFile().getParentFile());
        int exitCode = execute(sqlScript, wrapperScriptFile);

        tmpFile.delete();

        if (exitCode != EXIT_CODE_SUCCESS && !scriptFileName.equals(SHOW_INVALID_OBJECTS_SQL)) {
            logger.error(MessageFormat.format("Please execute script [{}] manually.", tmpFilePath));
            throw new ScriptExecException(errorMessage);
        }
    }
}
