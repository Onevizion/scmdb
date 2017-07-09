package com.onevizion.scmdb;

import com.onevizion.scmdb.vo.ScriptType;
import com.onevizion.scmdb.vo.SqlScript;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.FileUtils;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Date;

import static com.onevizion.scmdb.ColorLogger.Color.YELLOW;

public class SqlScriptExecutor {
    private static final String SQL_PLUS_COMMAND = "sql";
    private static final String INVALID_OBJECT_PREFIX = "Invalid objects in";
    private static final String INVALID_OBJECT_POSTFIX = "is invalid.";
    private static final String CANT_RUN_PROGRAM = "Cannot run program \"sql\"";
    private static final String ERROR_STARTING_AT_LINE = "Error starting at line :";
    private static final String CREATE_SQL = "create.sql";
    private boolean isErrorMsgStarted = false;

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
                if (line.startsWith(INVALID_OBJECT_PREFIX)) {
                    logger.warn(line, YELLOW);
                } else if (line.trim().endsWith(INVALID_OBJECT_POSTFIX)) {
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
        CommandLine commandLine = new CommandLine(SQL_PLUS_COMMAND);
        commandLine.addArgument("-L");
        commandLine.addArgument("-S");
        if (script.isUserSchemaScript()) {
            commandLine.addArgument(appArguments.getUserCredentials().getConnectionString());
        } else {
            commandLine.addArgument(appArguments.getOwnerCredentials().getConnectionString());
        }

        File workingDir = script.getFile().getParentFile();
        File wrapperScriptFile = getTmpWrapperScript(script.isUserSchemaScript(), workingDir);
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

    private File getTmpWrapperScript(boolean isUserSchemaScript, File workingDir) {
        ClassLoader classLoader = getClass().getClassLoader();
        URL wrapperScript;
        if (isUserSchemaScript) {
            wrapperScript = classLoader.getResource("sqlplus_exit_code_wrapper.sql");
        } else {
            wrapperScript = classLoader.getResource("compile_invalids_wrapper.sql");
        }
        File tmpFile = new File(workingDir.getAbsolutePath() + File.pathSeparator + "tmp.sql");

        try {
            FileUtils.copyURLToFile(wrapperScript, tmpFile);
        } catch (IOException e) {
            throw new RuntimeException("Can't copy tmp wrapper file.", e);
        }

        return tmpFile;
    }

    public boolean createDbScriptTable() {
        File scriptsDirectory = appArguments.getScriptsDirectory();
        String tmpFileName = String.valueOf(new Date().getTime()) + CREATE_SQL;
        String tmpFilePath = scriptsDirectory.getAbsolutePath() + File.pathSeparator + tmpFileName;
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

    public void printVersion() {
        CommandLine commandLine = new CommandLine(SQL_PLUS_COMMAND);
        commandLine.addArgument("-v");
        try {
            executor.execute(commandLine);
        } catch (Exception e) {
            logger.error("Error during command execution.", e);
        }
    }
}
