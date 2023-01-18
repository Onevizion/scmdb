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
    private static final String COMPILE_SCHEMAS_SQL = "compile_schemas.sql";
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
        return execute(script, false);
    }

    private int execute(SqlScript script, boolean isCompileSchemas) {
        DbCnnCredentials cnnCredentials = appArguments.getDbCredentials(script.getSchemaType());
        logger.info("\nExecuting script [{}] in schema [{}]. Start: {}", GREEN, script.getName(),
                cnnCredentials.getSchemaWithUrlBeforeDot(), ZonedDateTime.now().format(ISO_TIME));

        File workingDir = script.getFile().getParentFile();
        File wrapperScriptFile = getTmpWrapperScript(script.getSchemaType(), workingDir, isCompileSchemas);

        try (Connection connection = getConnection(script.getSchemaType(), cnnCredentials.getSchemaName())) {
            connection.setAutoCommit(false);
            ScriptExecutor executor = new ScriptExecutor(connection);
            ScriptRunnerContext ctx = new ScriptRunnerContext();

            ctx.setBaseConnection(connection);
            executor.setScriptRunnerContext(ctx);
            executor.setStmt(String.format(SQL_COMMAND, wrapperScriptFile.getAbsolutePath(),
                    script.getFile().getAbsolutePath()));

            Instant start = Instant.now();
            executor.run();
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

    private File getTmpWrapperScript(SchemaType schemaType, File workingDir, boolean isSkipInvalidsCompilation) {
        ClassLoader classLoader = getClass().getClassLoader();
        URL wrapperScript;
        if (schemaType.isCompileInvalids() && !isSkipInvalidsCompilation) {
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
        executeResourceScript(CREATE_SQL, false, "Can't create DB objects used by SCMDB.");
    }

    public void executeCompileSchemas() {
        executeResourceScript(COMPILE_SCHEMAS_SQL, true, "Can't compile invalid objects in _user, _rpt, _pkg schemas.");
    }

    private void executeResourceScript(String scriptFileName, boolean isCompileSchema, String errorMessage) {
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

        int exitCode = execute(sqlScript, isCompileSchema);
        if (exitCode != EXIT_CODE_SUCCESS) {
            logger.error("Please execute script \"" + tmpFilePath + "\" manually");
        }
        tmpFile.delete();
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

}
