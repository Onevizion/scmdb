package com.onevizion.scmdb;

import com.onevizion.scmdb.exception.ScriptExecException;
import com.onevizion.scmdb.vo.DbCnnCredentials;
import com.onevizion.scmdb.vo.SchemaType;
import com.onevizion.scmdb.vo.SqlScript;
import oracle.dbtools.raptor.newscriptrunner.ScriptExecutor;
import oracle.dbtools.raptor.newscriptrunner.ScriptRunnerContext;
import org.apache.commons.io.FileUtils;
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
import static com.onevizion.scmdb.Scmdb.EXIT_CODE_ERROR;
import static com.onevizion.scmdb.Scmdb.EXIT_CODE_SUCCESS;
import static com.onevizion.scmdb.vo.SchemaType.OWNER;
import static com.onevizion.scmdb.vo.ScriptType.COMMIT;
import static java.time.format.DateTimeFormatter.ISO_TIME;
import static oracle.dbtools.raptor.newscriptrunner.ScriptRunnerContext.*;
import static org.apache.commons.lang3.time.DurationFormatUtils.formatDurationHMS;

public class SqlScriptExecutor {
    private static final String SQL_COMMAND = "@%s %s";
    private static final String CREATE_SQL = "create.sql";
    private static final String COMPILE_SCHEMAS_SQL = "compile_schemas.sql";
    private static final int SCRIPT_EXIT_CODE_ERROR = 1;
    private static final int SCRIPT_EXIT_CODE_SUCCESS = 0;
    private static final Integer INVALID_OBJ_ERR_SQLCODE = 20001;
    private static final String ORA_REGEXP = "ORA-[0-9]*:.*";
    private static final String THROW_IF_INVALID_OBJECTS_SQL = "throw_if_invalid_objects.sql";

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
        File wrapperScriptFile = getTmpWrapperScript(script.getSchemaType().isCompileInvalids(),
                                                     appArguments.isIgnoreErrors(),
                                                     script.getFile().getParentFile());
        ScriptRunnerContext context = execute(script, wrapperScriptFile, false);
        return getExitCode(context);
    }

    private ScriptRunnerContext execute(SqlScript script, File wrapperScriptFile, boolean ignoreSqlLog) {
        DbCnnCredentials cnnCredentials = appArguments.getDbCredentials(script.getSchemaType());
        logger.info("\nExecuting script [{}] in schema [{}]. Start: {}", GREEN, script.getName(),
                    cnnCredentials.getSchemaWithUrlBeforeDot(), ZonedDateTime.now().format(ISO_TIME));

        try (Connection connection = getConnection(script.getSchemaType(), cnnCredentials.getSchemaName())) {
            connection.setAutoCommit(false);
            ScriptExecutor executor = new ScriptExecutor(connection);
            ScriptRunnerContext ctx = new ScriptRunnerContext();

            ctx.setBaseConnection(connection);
            if (ignoreSqlLog) {
                //stream is specified to ignore the sql stack code, when throw exception
                executor.setOut(new BufferedOutputStream(new ByteArrayOutputStream()));
            }
            executor.setScriptRunnerContext(ctx);
            executor.setStmt(String.format(SQL_COMMAND, wrapperScriptFile.getAbsolutePath(),
                    script.getFile().getAbsolutePath()));

            Instant start = Instant.now();
            executor.run();
            String scriptExecutionTime = formatDurationHMS(Duration.between(start, Instant.now()).toMillis());

            logger.info("\n[{}] runtime: {}", GREEN, script.getName(), scriptExecutionTime);

            return ctx;
        } catch (SQLException e) {
            logger.error("Error during connection DB.", e);
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
            throw new RuntimeException("Can't copy tmp wrapper file.", e);
        }

        return tmpFile;
    }

    public void createDbScriptTable() {
        executeResourceScript(CREATE_SQL, "Can't create DB objects used by SCMDB.", false);
    }

    public void executeCompileSchemas() {
        executeResourceScript(COMPILE_SCHEMAS_SQL, "Can't compile invalid objects in _user, _rpt, _pkg schemas.", false);
    }

    public void checkInvalidObjectAndThrow() {
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
        } catch (IOException e) {
            throw new RuntimeException("Can't copy " + scriptFileName + " file.", e);
        }

        SqlScript sqlScript = new SqlScript();
        sqlScript.setName(tmpFileName);
        sqlScript.setFile(tmpFile);
        sqlScript.setType(COMMIT);
        sqlScript.setSchemaType(OWNER);

        File wrapperScriptFile = getTmpWrapperScript(false, false, sqlScript.getFile().getParentFile());
        ScriptRunnerContext context = execute(sqlScript, wrapperScriptFile, ignoreSqlLog);
        tmpFile.delete();

        checkInvalidObjectAndThrow(context);
        int exitCode = getExitCode(context);
        if (exitCode != EXIT_CODE_SUCCESS) {
            logger.error("Please execute script \"" + tmpFilePath + "\" manually");
        }
        if (exitCode != EXIT_CODE_SUCCESS) {
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
            throw new RuntimeException(MessageFormat.format("Error during connection to the schema [{}].", schemaName),
                    exception);
        }
    }

    /**
     * If there is a custom exit code 20001 in the context, a list of invalid objects found will be displayed
     * and the application will end with code 1
     * @param context ScriptRunnerContext after executed script
     */
    private void checkInvalidObjectAndThrow(ScriptRunnerContext context) {
        if (context != null && INVALID_OBJ_ERR_SQLCODE.equals(context.getProperty(ERR_SQLCODE))) {
            String invalidObjMsg = String.valueOf(context.getProperty(ERR_MESSAGE_SQLCODE));
            if (invalidObjMsg != null && !invalidObjMsg.isEmpty()) {
                String invalidObjectMessage = invalidObjMsg.replaceAll(ORA_REGEXP, "");
                System.err.print(invalidObjectMessage);
                System.exit(EXIT_CODE_ERROR);
            }
        }
    }

    private int getExitCode(ScriptRunnerContext context) {
        return context != null
                && !(boolean) context.getProperty(ERR_ENCOUNTERED) ? SCRIPT_EXIT_CODE_SUCCESS : SCRIPT_EXIT_CODE_ERROR;
    }

}
