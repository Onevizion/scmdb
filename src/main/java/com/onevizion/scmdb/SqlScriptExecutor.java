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
import org.apache.commons.lang3.SystemUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;
import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
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
    private static final String SQL_COMMAND = "@%s %s %s";
    private static final String CREATE_SQL = "create.sql";
    private static final String COMPILE_SCHEMAS_SQL = "compile_schemas.sql";
    private static final String SHOW_INVALID_OBJECTS_SQL = "check_invalid_objects.sql";
    private static final String DISABLE_JOBS_SQL = "disable_jobs.sql";
    private static final String ENABLE_JOBS_SQL = "enable_jobs.sql";
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
            logger.warn("Unable to check invalid objects: [{}]", ColorLogger.Color.YELLOW, e.getMessage());
        }
    }

    public int execute(SqlScript script) {
        boolean isPackageScript = script.getSchemaType().isCompileInvalids() && isPackageScript(script);
        File scriptsDirectory = appArguments.getScriptsDirectory();
        if (scriptsDirectory == null) {
            scriptsDirectory = SystemUtils.getJavaIoTmpDir();
        }

        File workingDirectory;
        try {
            workingDirectory = script.getResource().isFile()
                    ? script.getResource().getFile().getParentFile()
                    : scriptsDirectory;
        } catch (IOException e) {
            throw new RuntimeException("Unable to get script file from resource for [" + script.getResource() + "]", e);
        }

        File wrapperScriptFile = getTmpWrapperScript(script.getSchemaType().isCompileInvalids(),
                                                     appArguments.isIgnoreErrors(),
                                                     workingDirectory);
        return execute(script, wrapperScriptFile, isPackageScript);
    }

    private int execute(SqlScript script, File wrapperScriptFile, boolean isPackageScript) {
        DbCnnCredentials cnnCredentials = appArguments.getDbCredentials(script.getSchemaType());
        logger.info("\nExecuting script [{}] in schema [{}]. Start: {}", GREEN, script.getName(),
                cnnCredentials.getSchemaWithUrlBeforeDot(), ZonedDateTime.now().format(ISO_TIME));

        File scriptFile;
        if (script.getResource().isFile()) {
            try {
                scriptFile = script.getResource().getFile();
            } catch (IOException e) {
                throw new RuntimeException("Unable to get File reference for [" + script.getResource() + "]", e);
            }
        } else {
            try {
                scriptFile = Files.createTempFile("scmdb_classpath", ".sql").toFile();
                scriptFile.deleteOnExit();
            } catch (IOException e) {
                throw new RuntimeException("Unable to create temporarily file for [" + script.getResource() + "]", e);
            }

            try (InputStream inputStream = script.getResource().getInputStream()) {
                FileUtils.copyInputStreamToFile(inputStream, scriptFile);
            } catch (IOException e) {
                throw new RuntimeException("Unable to copy resource [" + script.getResource() + "] to temporary file [" + scriptFile + "]", e);
            }
        }

        try (Connection connection = getConnection(script.getSchemaType(), cnnCredentials.getSchemaName());
             ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            connection.setAutoCommit(false);
            ScriptExecutor executor = new ScriptExecutor(connection);
            ScriptRunnerContext ctx = new ScriptRunnerContext();

            ctx.setBaseConnection(connection);
            ctx.setOutputStreamWrapper(new BufferedOutputStream(new TeeOutputStream(System.out, outputStream)));
            executor.setScriptRunnerContext(ctx);

            // Pass parameter: 1 = enable pkg_audit_comp (regular script), 0 = don't enable (package script)
            String enableLockedCompsMod = (!isPackageScript && script.getSchemaType().isCompileInvalids()) ? "1" : "0";
            executor.setStmt(String.format(SQL_COMMAND, wrapperScriptFile.getAbsolutePath(),
                                           scriptFile.getAbsolutePath(),
                                           enableLockedCompsMod));

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

        File tmpFile = new File(workingDir.getAbsolutePath(), System.currentTimeMillis() + "tmp.sql");
        tmpFile.deleteOnExit();

        try {
            FileUtils.copyURLToFile(wrapperScript, tmpFile);
        } catch (IOException e) {
            throw new RuntimeException("Can't copy tmp wrapper file.", e);
        }

        return tmpFile;
    }

    private boolean isPackageScript(SqlScript script) {
        String scriptText = script.getText();
        return ScriptHelper.isPackageScript(scriptText);
    }

    public void createDbScriptTable() {
        executeResourceScript(CREATE_SQL, "Can't create DB objects used by SCMDB.");
    }

    public void executeCompileSchemas() {
        executeResourceScript(COMPILE_SCHEMAS_SQL, "Can't compile invalid objects in _user, _rpt, _pkg schemas.");
    }

    public void disableJobs() {
        executeResourceScript(DISABLE_JOBS_SQL, "Can't disable database jobs.");
    }

    public void enableJobs() {
        executeResourceScript(ENABLE_JOBS_SQL, "Can't enable database jobs.");
    }

    private Connection getConnection(SchemaType schemaType, String schemaName) {
        try {
            return switch (schemaType) {
                case USER -> userDataSource.getConnection();
                case RPT -> rptDataSource.getConnection();
                case PKG -> pkgDataSource.getConnection();
                case OWNER -> dataSource.getConnection();
                case PERFSTAT -> throw new UnsupportedOperationException("PERFSTAT schema is not supported in this context.");
            };
        } catch (SQLException exception) {
            throw new DbConnectionException(
                    StringPlaceholderUtils.replace("Error during connection to the schema [{}].", schemaName),
                    exception);
        }
    }

    private void executeResourceScript(String scriptFileName, String errorMessage, boolean ignoreSqlLog) {
        File scriptsDirectory = appArguments.getScriptsDirectory();
        if (scriptsDirectory == null) {
            scriptsDirectory = SystemUtils.getJavaIoTmpDir();
        }

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
        sqlScript.setResource(new FileSystemResource(tmpFile));
        sqlScript.setType(COMMIT);
        sqlScript.setSchemaType(OWNER);

        File wrapperScriptFile = getTmpWrapperScript(false, false, tmpFile.getParentFile());
        int exitCode = execute(sqlScript, wrapperScriptFile, false);

        tmpFile.delete();

        if (exitCode != EXIT_CODE_SUCCESS && !scriptFileName.equals(SHOW_INVALID_OBJECTS_SQL)) {
            logger.error("Please execute script [{}] manually.", tmpFilePath);
            throw new ScriptExecException(errorMessage);
        }
    }
}
