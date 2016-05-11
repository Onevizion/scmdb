package com.onevizion.scmdb;

import com.onevizion.scmdb.vo.DbCnnCredentials;
import org.apache.commons.exec.*;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class SqlScriptExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SqlScriptExecutor.class);
    private static final String SQLPLUS_COMMAND = "sqlplus";
    private Executor executor;
    private DbCnnCredentials dbCnnCredentials;

    public SqlScriptExecutor(DbCnnCredentials dbCnnCredentials) {
        this.dbCnnCredentials = dbCnnCredentials;
        executor = new DefaultExecutor();
        executor.setStreamHandler(new PumpStreamHandler(System.out));
    }

    public int execute(File sqlScript) {
        executor.setWorkingDirectory(sqlScript.getParentFile());

        CommandLine commandLine = new CommandLine(SQLPLUS_COMMAND);
        commandLine.addArgument("-L");
        commandLine.addArgument("-S");
        boolean isUserSchemaScript = isUserSchemaScript(sqlScript.getName());
        if (isUserSchemaScript) {
            commandLine.addArgument(dbCnnCredentials.getUserCnnStr());
        } else {
            commandLine.addArgument(dbCnnCredentials.getOwnerCnnStr());
        }

        File wrapperScriptFile = getWrapperScript(isUserSchemaScript);
        commandLine.addArgument("@" + wrapperScriptFile.getAbsolutePath());
        commandLine.addArgument(sqlScript.getAbsolutePath());

        try {
            return executor.execute(commandLine);
        } catch (ExecuteException e) {
            logger.error("Script executed with errors", e);
            return e.getExitValue();
        } catch (IOException e) {
            logger.error("Error during command execution", e);
            return 1;
        }
    }

    private File getWrapperScript(boolean isUserSchemaScript) {
        ClassLoader classLoader = getClass().getClassLoader();
        File wrapperScript;
        if (isUserSchemaScript) {
            wrapperScript = new File(classLoader.getResource("sqlplus_exit_code_wrapper.sql").getFile());
        } else {
            wrapperScript = new File(classLoader.getResource("compile_invalids_wrapper.sql").getFile());
        }

        return wrapperScript;
    }

    private boolean isUserSchemaScript(String scriptFileName) {
        String scriptName = FilenameUtils.getBaseName(scriptFileName);
        return scriptName.endsWith("_user") && !scriptName.endsWith("pkg_user");
    }
}
