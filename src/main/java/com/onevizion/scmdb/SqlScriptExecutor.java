package com.onevizion.scmdb;

import com.onevizion.scmdb.vo.DbCnnCredentials;
import com.onevizion.scmdb.vo.ScriptType;
import com.onevizion.scmdb.vo.SqlScript;
import org.apache.commons.exec.*;
import org.apache.commons.io.FileUtils;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Date;

import static com.onevizion.scmdb.ColorLogger.Color.GREEN;
import static com.onevizion.scmdb.ColorLogger.Color.YELLOW;

public class SqlScriptExecutor {
    private static final String SQL_CLIENT_COMMAND = "sql";

    private static final String INVALID_OBJECT_PREFIX = "Invalid objects in";
    private static final String INVALID_OBJECT_REGEX = "^(\\w+\\s){0,2}\\w+\\s+\\S+\\s+is invalid.\\s*";

    private boolean isSqlClBannerStarted = false;
    private static final String SQLCL_BANNER_START_REGEX = "^SQLcl: Release [\\d|.]+ Production on .+";
    private static final String SQLCL_BANNER_ORA_VERSION_REGEX = "Oracle Database [\\w|\\d]+ \\w+ \\w+ Release [\\d|.]+ - \\d+bit Production";
    private static final String SQLCL_BANNER_END_REGEX = "^" + SQLCL_BANNER_ORA_VERSION_REGEX;
    private static final String SQLCL_DISCON_FROM_DB_REGEX = "^Disconnected from " + SQLCL_BANNER_ORA_VERSION_REGEX;

    private boolean isErrorMsgStarted = false;
    private static final String ERROR_STARTING_AT_LINE = "Error starting at line :";

    private static final String CANT_RUN_PROGRAM = "Cannot run program \"sql\"";
    private static final String CREATE_SQL = "create.sql";

    private Executor executor;

    @Resource
    private AppArguments appArguments;

    @Resource
    private ColorLogger logger;

    public SqlScriptExecutor() {
        executor = new DefaultExecutor();
        executor.setStreamHandler(new PumpStreamHandler(new LogOutputStream() {
            @Override
            protected void processLine(String line, int logLevel) {
                if (isSqlClBannerPrinted(line)) {
                    return;
                } else if (line.startsWith(INVALID_OBJECT_PREFIX)) {
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
        logger.info("\nExecuting script [{}] in schema [{}]", GREEN, script.getName(), cnnCredentials.getSchemaName());

        CommandLine commandLine = new CommandLine(SQL_CLIENT_COMMAND);
        commandLine.addArgument("-L");
        commandLine.addArgument(cnnCredentials.getConnectionString());

        File workingDir = script.getFile().getParentFile();
        File wrapperScriptFile = getTmpWrapperScript(script.getSchemaType().isCompileInvalids(), workingDir);
        commandLine.addArgument("@" + wrapperScriptFile.getAbsolutePath());
        commandLine.addArgument(script.getFile().getAbsolutePath());

        executor.setWorkingDirectory(workingDir);
        try {
            return executor.execute(commandLine);
        } catch (ExecuteException e) {
            return e.getExitValue();
        } catch (IOException e) {
            if (e.getMessage().startsWith(CANT_RUN_PROGRAM)) {
                return 2;
            } else {
                logger.error("Error during command execution.", e);
                return 1;
            }
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

    public boolean createDbScriptTable() {
        File scriptsDirectory = appArguments.getScriptsDirectory();
        String tmpFileName = new Date().getTime() + CREATE_SQL;
        String tmpFilePath = scriptsDirectory.getAbsolutePath() + File.separator + tmpFileName;
        File tmpFile = new File(tmpFilePath);
        URL resource = getClass().getClassLoader().getResource(CREATE_SQL);
        try {
            FileUtils.copyURLToFile(resource, tmpFile);
        } catch (IOException e) {
            throw new RuntimeException("Can't copy " + CREATE_SQL +" file.", e);
        }

        SqlScript sqlScript = new SqlScript();
        sqlScript.setName(tmpFileName);
        sqlScript.setFile(tmpFile);
        sqlScript.setType(ScriptType.COMMIT);

        int exitCode = execute(sqlScript);
        tmpFile.delete();
        return exitCode == 0;
    }

    public void printVersion() throws IOException {
        CommandLine commandLine = new CommandLine(SQL_CLIENT_COMMAND);
        commandLine.addArgument("-v");
        try {
            executor.execute(commandLine);
        } catch (IOException e) {
            logger.error("Error during command execution.", e);
            throw e;
        }
    }

    private boolean isSqlClBannerPrinted(String line) {
        if (line.matches(SQLCL_BANNER_START_REGEX)) {
            isSqlClBannerStarted = true;
        } else if (line.matches(SQLCL_BANNER_END_REGEX)) {
            isSqlClBannerStarted = false;
            return true;
        } else if (isSqlClBannerStarted || line.matches(SQLCL_DISCON_FROM_DB_REGEX)) {
            return true;
        }

        return isSqlClBannerStarted;
    }
}
