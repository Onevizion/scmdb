package com.onevizion.scmdb;

import com.onevizion.scmdb.exception.DbConnectionException;
import com.onevizion.scmdb.exception.InvalidObjectException;
import com.onevizion.scmdb.exception.ScriptExecException;
import com.onevizion.scmdb.vo.DbCnnCredentials;
import com.onevizion.scmdb.vo.SchemaType;
import com.onevizion.scmdb.vo.ScriptStatus;
import com.onevizion.scmdb.vo.SqlScript;
import oracle.dbtools.raptor.newscriptrunner.ScriptExecutor;
import oracle.dbtools.raptor.newscriptrunner.ScriptRunnerContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.TeeOutputStream;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.io.*;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;

import static com.onevizion.scmdb.ColorLogger.Color.GREEN;
import static com.onevizion.scmdb.vo.SchemaType.OWNER;
import static com.onevizion.scmdb.vo.ScriptStatus.EXECUTED;
import static com.onevizion.scmdb.vo.ScriptStatus.EXECUTED_WITH_ERRORS;
import static com.onevizion.scmdb.vo.ScriptType.COMMIT;
import static java.time.format.DateTimeFormatter.ISO_TIME;
import static oracle.dbtools.raptor.newscriptrunner.ScriptRunnerContext.*;
import static org.apache.commons.lang3.time.DurationFormatUtils.formatDurationHMS;

public class SqlScriptExecutor {
    private static final String SQL_COMMAND = "@%s %s";
    private static final String CREATE_SQL = "create.sql";
    private static final String COMPILE_SCHEMAS_SQL = "compile_schemas.sql";
    private static final Integer INVALID_OBJ_ERR_SQLCODE = 20001;
    private static final String ORA_REGEXP = "ORA-[0-9]*:.*";
    private static final String THROW_IF_INVALID_OBJECTS_SQL = "throw_if_invalid_objects.sql";
    private static final String ERROR_CONNECTION_TO_THE_SCHEMA = "Error during connection to the schema [{0}].";

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

    public ScriptStatus execute(SqlScript script) {
        File wrapperScriptFile = getTmpWrapperScript(script.getSchemaType().isCompileInvalids(),
                                                     appArguments.isIgnoreErrors(),
                                                     script.getFile().getParentFile());
        ScriptRunnerContext context = execute(script, wrapperScriptFile, false);
        return getScriptStatus(context);
    }

    private ScriptRunnerContext execute(SqlScript script, File wrapperScriptFile, boolean ignoreSqlLog) {
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
            if (ignoreSqlLog) {
                //stream is specified to ignore the sql stack code, when throw exception
                executor.setOut(new BufferedOutputStream(new ByteArrayOutputStream()));
            }
            executor.setScriptRunnerContext(ctx);
            executor.setStmt(String.format(SQL_COMMAND, wrapperScriptFile.getAbsolutePath(),
                                           script.getFile().getAbsolutePath()));

            Instant start = Instant.now();
            executor.run();
            script.setOutput(outputStream.toString());

            String scriptExecutionTime = formatDurationHMS(Duration.between(start, Instant.now()).toMillis());

            logger.info("\n[{}] runtime: {}", GREEN, script.getName(), scriptExecutionTime);

            return ctx;
        } catch (SQLException e) {
            logger.error("Error during connection DB.", e);
            return null;

        } catch (IOException e) {
            logger.error("Can't close OutputStream", e);
            return null;

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
            throw new ScriptExecException("Can't copy tmp wrapper file.", e);
        }

        return tmpFile;
    }

    public void createDbScriptTable() {
        executeResourceScript(CREATE_SQL, "Can't create DB objects used by SCMDB.", false);
    }

    public void executeCompileSchemas() {
        executeResourceScript(COMPILE_SCHEMAS_SQL, "Can't compile invalid objects in _user, _rpt, _pkg schemas.", false);
    }

    public void checkInvalidObjectOrThrow() {
        executeResourceScript(THROW_IF_INVALID_OBJECTS_SQL, "Can't execute invalid objects check.", true);
    }

    private void executeResourceScript(String scriptFileName, String errorMessage, boolean ignoreSqlLog) {
        File scriptsDirectory = appArguments.getScriptsDirectory();
        String tmpFileName = new Date().getTime() + scriptFileName;
        String tmpFilePath = scriptsDirectory.getAbsolutePath() + File.separator + tmpFileName;
        File tmpFile = new File(tmpFilePath);
        URL resource = getClass().getClassLoader().getResource(scriptFileName);
        try {
            FileUtils.copyURLToFile(resource, tmpFile);
        } catch (IOException exception) {
            throw new ScriptExecException("Can't copy " + scriptFileName + " file.", exception);
        }

        SqlScript sqlScript = new SqlScript();
        sqlScript.setName(tmpFileName);
        sqlScript.setFile(tmpFile);
        sqlScript.setType(COMMIT);
        sqlScript.setSchemaType(OWNER);

        File wrapperScriptFile = getTmpWrapperScript(false, false, sqlScript.getFile().getParentFile());
        ScriptRunnerContext context = execute(sqlScript, wrapperScriptFile, ignoreSqlLog);
        tmpFile.delete();

        checkInvalidObjectOrThrow(context);
        ScriptStatus scriptStatus = getScriptStatus(context);
        if (scriptStatus == EXECUTED_WITH_ERRORS) {
            logger.error("Please execute script \"" + tmpFilePath + "\" manually");
            throw new ScriptExecException(errorMessage);
        }
    }

    private Connection getConnection(SchemaType schemaType, String schemaName) {
        try {
            switch (schemaType) {
                case USER:
                    return userDataSource.getConnection();
                case RPT:
                    return rptDataSource.getConnection();
                case PKG:
                    return pkgDataSource.getConnection();
                default:
                    return dataSource.getConnection();
            }
        } catch (SQLException exception) {
            throw new DbConnectionException(MessageFormat.format(ERROR_CONNECTION_TO_THE_SCHEMA, schemaName), exception);
        }
    }

    /**
     * The method checks the context for invalid objects.
     * If the context contains custom exit code 20001, an InvalidObjectException will be thrown.
     * @param context ScriptRunnerContext after executed script
     * @throws InvalidObjectException if db contains invalid object
     */
    private void checkInvalidObjectOrThrow(ScriptRunnerContext context) {
        if (context != null && INVALID_OBJ_ERR_SQLCODE.equals(context.getProperty(ERR_SQLCODE))) {
            String invalidObjMsg = String.valueOf(context.getProperty(ERR_MESSAGE_SQLCODE));
            if (invalidObjMsg != null && !invalidObjMsg.isEmpty()) {
                String invalidObjectMessage = invalidObjMsg.replaceAll(ORA_REGEXP, "");
                throw new InvalidObjectException(invalidObjectMessage);
            }
        }
    }

    private ScriptStatus getScriptStatus(ScriptRunnerContext context) {
        return context != null && !(boolean) context.getProperty(ERR_ENCOUNTERED)
                ? EXECUTED
                : EXECUTED_WITH_ERRORS;
    }

}
