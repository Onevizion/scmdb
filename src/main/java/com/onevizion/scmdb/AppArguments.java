package com.onevizion.scmdb;

import com.onevizion.scmdb.vo.DbCnnCredentials;

import java.io.File;

public class AppArguments {
    private static final String DB_SCRIPT_DIR_ERROR_MESSAGE = "You should specify absolute path to db scripts";
    private static final String ARGUMENTS_ERROR_MESSAGE = "You can't specify both -genDdl and -exec. Choose one.";
    private static final String ARG_GEN_DDL = "-genDdl";
    private static final String ARG_EXECUTE_SCRIPTS = "-exec";

    private File scriptDirectory;
    private DbCnnCredentials ownerCredentials;
    private DbCnnCredentials userCredentials;
    private boolean isGenDdl;
    private boolean isExecuteScripts;

    private AppArguments() {}

    public AppArguments(String[] args) {
        parse(args);
    }

    public void parse(String[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException(DbCnnCredentials.DB_CNN_STR_ERROR_MESSAGE + "\n" + DB_SCRIPT_DIR_ERROR_MESSAGE);
        }
        ownerCredentials = DbCnnCredentials.create(args[0]);
        userCredentials = DbCnnCredentials.create(args[0]);
        scriptDirectory = parseDbScriptDir(args[1]);

        if (args.length > 2) {
            for (int i = 2; i < args.length; i++) {
                String arg = args[i];
                if (ARG_GEN_DDL.equals(arg)) {
                    if (isExecuteScripts) {
                        throw new IllegalArgumentException(ARGUMENTS_ERROR_MESSAGE);
                    }
                    isGenDdl = true;
                } else if (ARG_EXECUTE_SCRIPTS.equals(arg)) {
                    if (isGenDdl) {
                        throw new IllegalArgumentException(ARGUMENTS_ERROR_MESSAGE);
                    }
                    isExecuteScripts = true;
                }
            }
        }
    }

    private File parseDbScriptDir(String path) {
        File f = new File(path);
        if (f.exists()) {
            return f;
        } else {
            throw new IllegalArgumentException("File or directory doesn't exist: " + path);
        }
    }

    public File getScriptDirectory() {
        return scriptDirectory;
    }

    public void setScriptDirectory(File scriptDirectory) {
        this.scriptDirectory = scriptDirectory;
    }

    public DbCnnCredentials getOwnerCredentials() {
        return ownerCredentials;
    }

    public void setOwnerCredentials(DbCnnCredentials ownerCredentials) {
        this.ownerCredentials = ownerCredentials;
    }

    public DbCnnCredentials getUserCredentials() {
        return userCredentials;
    }

    public void setUserCredentials(DbCnnCredentials userCredentials) {
        this.userCredentials = userCredentials;
    }

    public boolean isGenDdl() {
        return isGenDdl;
    }

    public void setGenDdl(boolean genDdl) {
        isGenDdl = genDdl;
    }

    public boolean isExecuteScripts() {
        return isExecuteScripts;
    }

    public void setExecuteScripts(boolean executeScripts) {
        isExecuteScripts = executeScripts;
    }
}
