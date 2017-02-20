package com.onevizion.scmdb;

import com.onevizion.scmdb.vo.SqlScript;
import org.apache.commons.exec.*;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.net.URL;

public class SqlScriptExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SqlScriptExecutor.class);
    private static final String SQL_PLUS_COMMAND = "sqlplus";
    private Executor executor;

    @Resource
    private AppArguments appArguments;

    public SqlScriptExecutor() {
        executor = new DefaultExecutor();
        executor.setStreamHandler(new PumpStreamHandler(new LogOutputStream() {
            @Override
            protected void processLine(String line, int logLevel) {
                logger.info(line);
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
            logger.error("Error during command execution.", e);
            return 1;
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
}
