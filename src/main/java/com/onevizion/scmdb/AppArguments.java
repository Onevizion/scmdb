package com.onevizion.scmdb;

import com.onevizion.scmdb.vo.DbCnnCredentials;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;

import static java.util.Arrays.asList;

public class AppArguments {
    private File scriptsDirectory;
    private File ddlsDirectory;
    private DbCnnCredentials ownerCredentials;
    private DbCnnCredentials userCredentials;
    private DbCnnCredentials rptCredentials;
    private boolean genDdl;
    private boolean executeScripts;
    private boolean useColorLogging = true;
    private boolean all = false;

    private final static String DDL_DIRECTORY_NAME = "ddl";

    private AppArguments() {}

    public AppArguments(String[] args) {
        parse(args);
    }

    public void parse(String[] args) {
        OptionParser parser = new OptionParser();
        OptionSpec<String> ownerSchemaOption = parser.accepts("owner-schema").withRequiredArg().ofType(String.class);
        OptionSpec<String> userSchemaOption = parser.accepts("user-schema").withRequiredArg().ofType(String.class);
        OptionSpec<String> rptSchemaOption = parser.accepts("rpt-schema").withRequiredArg().ofType(String.class);
        OptionSpec<File> scriptsDirectoryOption = parser.accepts("scripts-dir").withRequiredArg().ofType(File.class);

        OptionSpec execOption = parser.acceptsAll(asList("e", "exec"));
        OptionSpec genDdlOption = parser.acceptsAll(asList("d", "gen-ddl"));
        OptionSpec allOption = parser.acceptsAll(asList("a", "all"));
        OptionSpec noColorOption = parser.acceptsAll(asList("n", "no-color"));

        OptionSet options = parser.parse(args);

        if(!options.has(ownerSchemaOption) || !options.has(scriptsDirectoryOption)){
            throw new IllegalArgumentException("--owner-schema and --scripts-dir are required parameters.");
        }

        ownerCredentials = DbCnnCredentials.create(options.valueOf(ownerSchemaOption));
        if (options.has(userSchemaOption)) {
            userCredentials = DbCnnCredentials.create(options.valueOf(userSchemaOption));
        } else {
            userCredentials = DbCnnCredentials.create(DbCnnCredentials.genUserCnnStr(ownerCredentials.getConnectionString()));
        }
        if (options.has(rptSchemaOption)) {
            rptCredentials = DbCnnCredentials.create(options.valueOf(rptSchemaOption));
        } else {
            rptCredentials = DbCnnCredentials.create(DbCnnCredentials.genRptCnnStr(ownerCredentials.getConnectionString()));
        }
        scriptsDirectory = options.valueOf(scriptsDirectoryOption);
        if (!scriptsDirectory.exists() || !scriptsDirectory.isDirectory()) {
            throw new IllegalArgumentException("Path [" + scriptsDirectory.getAbsolutePath() + "] doesn't exists or isn't a directory." +
                    " [--scripts-dir] should contains absolute path and points to scripts directory");
        }
        if(options.has(genDdlOption)){
            ddlsDirectory = new File(scriptsDirectory.getParentFile().getAbsolutePath() + File.separator +
                    DDL_DIRECTORY_NAME);
            if (!ddlsDirectory.exists() || !ddlsDirectory.isDirectory()) {
                throw new IllegalArgumentException("Path [" + ddlsDirectory.getAbsolutePath() + "] doesn't exists or isn't a directory." +
                        " Can't find ddl directory");
            }
        }

        if (options.has(execOption) && options.has(genDdlOption)) {
            throw new IllegalArgumentException("You can't specify both --gen-ddl and --exec arguments. Choose one.");
        }
        executeScripts = options.has(execOption);
        genDdl = options.has(genDdlOption);
        all = options.has(allOption);
        useColorLogging = !options.has(noColorOption);
    }

    public File getScriptsDirectory() {
        return scriptsDirectory;
    }

    public File getDdlsDirectory() {
        return ddlsDirectory;
    }

    public DbCnnCredentials getOwnerCredentials() {
        return ownerCredentials;
    }

    public DbCnnCredentials getUserCredentials() {
        return userCredentials;
    }

    public DbCnnCredentials getRptCredentials() {
        return rptCredentials;
    }

    public boolean isGenDdl() {
        return genDdl;
    }

    public boolean isExecuteScripts() {
        return executeScripts;
    }

    public boolean isUseColorLogging() {
        return useColorLogging;
    }

    public boolean isAll() {
        return all;
    }
}
