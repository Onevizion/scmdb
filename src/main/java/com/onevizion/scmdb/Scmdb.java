package com.onevizion.scmdb;

import com.onevizion.scmdb.vo.DbCnnCredentials;

import java.io.File;

public class Scmdb {
    private static final String DB_SCRIPT_DIR_ERROR_MESSAGE = "You should specify absolute path to db scripts";
    private static final String ARGUMENTS_ERROR_MESSAGE = "You can't specify both -genDdl and -exec. Choose one.";
    private static final String ARG_GEN_DDL = "-genDdl";
    private static final String ARG_EXECUTE_SCRIPTS = "-exec";

    public static void main(String[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException(DbCnnCredentials.DB_CNN_STR_ERROR_MESSAGE + "\n" + DB_SCRIPT_DIR_ERROR_MESSAGE);
        }
        DbCnnCredentials cnnCredentials = DbCnnCredentials.create(args[0]);
        File scriptDir = parseDbScriptDir(args[1]);

        boolean isGenDdl = false;
        boolean isExecScripts = false;

        if (args.length > 2) {
            for (int i = 2; i < args.length; i++) {
                String arg = args[i];
                if (ARG_GEN_DDL.equals(arg)) {
                    if (isExecScripts) {
                        throw new IllegalArgumentException(ARGUMENTS_ERROR_MESSAGE);
                    }
                    isGenDdl = true;
                } else if (ARG_EXECUTE_SCRIPTS.equals(arg)) {
                    if (isGenDdl) {
                        throw new IllegalArgumentException(ARGUMENTS_ERROR_MESSAGE);
                    }
                    isExecScripts = true;
                }
            }
        }

        Checkouter checkouter = new Checkouter(cnnCredentials, scriptDir, isGenDdl, isExecScripts);
        checkouter.checkout();
    }

    private static File parseDbScriptDir(String path) {
        File f = new File(path);
        if (f.exists()) {
            return f;
        } else {
            throw new IllegalArgumentException("File or directory doesn't exist: " + path);
        }
    }
}