package com.onevizion.scmdb;

import com.onevizion.scmdb.exception.ScriptExecException;
import com.onevizion.scmdb.vo.DbCnnCredentials;
import com.onevizion.scmdb.vo.SqlScript;
import org.apache.commons.exec.*;
import org.apache.commons.io.FileUtils;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.net.URL;
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
import static org.apache.commons.lang3.time.DurationFormatUtils.formatDurationHMS;

public class SqlScriptExecutor {
    private static final String SQL_CLIENT_COMMAND = "sql";

    private static final String INVALID_OBJECT_PREFIX = "Invalid objects in";
    private static final String INVALID_OBJECT_REGEX = "^(\\w+\\s){0,2}\\w+\\s+\\S+\\s+is invalid.\\s*";
    private static final String ERROR_STARTING_AT_LINE = "Error starting at line :";
    private static final String CREATE_SQL = "create.sql";
    private static final int SCRIPT_EXIT_CODE_ERROR = 1;

    private Executor executor;
    private boolean isErrorMsgStarted = false;

    @Resource
    private AppArguments appArguments;

    @Resource
    private ColorLogger logger;

    public SqlScriptExecutor() {
        executor = new DefaultExecutor();
        executor.setStreamHandler(new PumpStreamHandler(new LogOutputStream() {
            @Override
            protected void processLine(String line, int logLevel) {
                if (line.startsWith(INVALID_OBJECT_PREFIX)) {
                    logger.warn(line, YELLOW);
                } else if (line.matches(INVALID_OBJECT_REGEX)) {
                    logger.warn(line, YELLOW);
                } else if (line.startsWith(ERROR_STARTING_AT_LINE)) {
                    isErrorMsgStarted = true;
                    logger.error(line);
                } else if (isErrorMsgStarted) {
                    logger.error(line);
                } else {
                    logger.info(line);
                }
            }
        }, new LogOutputStream() {
            @Override
            protected void processLine(String line, int logLevel) {
                logger.error(line);
            }
        }));
    }

    public int execute(SqlScript script) {
        DbCnnCredentials cnnCredentials = appArguments.getDbCredentials(script.getSchemaType());
        logger.info("\nExecuting script [{}] in schema [{}]. Start: {}", GREEN, script.getName(),
                cnnCredentials.getSchemaWithUrlBeforeDot(), ZonedDateTime.now().format(ISO_TIME));

        CommandLine commandLine = new CommandLine(SQL_CLIENT_COMMAND);
        commandLine.addArgument("-S");
        commandLine.addArgument("-L");
        commandLine.addArgument(cnnCredentials.getConnectionString());

        File workingDir = script.getFile().getParentFile();
        File wrapperScriptFile = getTmpWrapperScript(script.getSchemaType().isCompileInvalids(), workingDir);
        commandLine.addArgument("@" + wrapperScriptFile.getAbsolutePath());
        commandLine.addArgument(script.getFile().getAbsolutePath());

        executor.setWorkingDirectory(workingDir);
        try {
            Instant start = Instant.now();
            int exitCode = executor.execute(commandLine);
            String scriptExecutionTime = formatDurationHMS(Duration.between(start, Instant.now()).toMillis());
            logger.info("\n[{}] runtime: {}", GREEN, script.getName(), scriptExecutionTime);
            return exitCode;
        } catch (ExecuteException e) {
            return e.getExitValue();
        } catch (IOException e) {
            logger.error("Error during command execution.", e);
            return SCRIPT_EXIT_CODE_ERROR;
        } finally {
            wrapperScriptFile.delete();
        }
    }

    private File getTmpWrapperScript(boolean compileInvalids, File workingDir) {
        ClassLoader classLoader = getClass().getClassLoader();
        URL wrapperScript;
        if (compileInvalids) {
            wrapperScript = classLoader.getResource("compile_invalids_wrapper.sql");
        } else {
            wrapperScript = classLoader.getResource("sqlplus_exit_code_wrapper.sql");
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

    public void printVersion() {
        CommandLine commandLine = new CommandLine(SQL_CLIENT_COMMAND);
        commandLine.addArgument("-v");
        try {
            executor.execute(commandLine);
        } catch (IOException e) {
            logger.error("Error during command execution.", e);
            throw new ScriptExecException("Cannot find SQLcl, make sure SQLcl is available.", e);
        }
    }
}
