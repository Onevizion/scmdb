package com.onevizion.scmdb;

import com.onevizion.scmdb.vo.DbCnnCredentials;
import com.onevizion.scmdb.vo.SchemaType;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static com.onevizion.scmdb.vo.SchemaType.*;
import static java.util.Arrays.asList;

public class AppArguments {
    private File scriptsDirectory;
    private File ddlsDirectory;
    private File packageDirectory;
    private Map<SchemaType, DbCnnCredentials> credentials = new HashMap<>();
    private boolean genDdl;
    private boolean executeScripts;
    private boolean genPackage;
    private boolean useColorLogging = true;
    private boolean all = false;
    private boolean omitChanged = false;
    private boolean ignoreErrors = false;
    private boolean backport = false;

    private final static String DDL_DIRECTORY_NAME = "ddl";
    private final static String PACKAGES_DIRECTORY_NAME = "packages";

    private AppArguments() {}

    public AppArguments(String[] args) {
        parse(args);
    }

    void parse(String[] args) {
        OptionParser parser = new OptionParser();
        OptionSpec<String> ownerSchemaOption = parser.accepts("owner-schema").withRequiredArg().ofType(String.class);
        OptionSpec<String> userSchemaOption = parser.accepts("user-schema").withOptionalArg().ofType(String.class);
        OptionSpec<String> rptSchemaOption = parser.accepts("rpt-schema").withOptionalArg().ofType(String.class);
        OptionSpec<String> pkgSchemaOption = parser.accepts("pkg-schema").withOptionalArg().ofType(String.class);
        OptionSpec<String> perfstatSchemaOption = parser.accepts("perfstat-schema").withOptionalArg().ofType(String.class);
        OptionSpec<File> scriptsDirectoryOption = parser.accepts("scripts-dir").withRequiredArg().ofType(File.class);

        OptionSpec execOption = parser.acceptsAll(asList("e", "exec"));
        OptionSpec genDdlOption = parser.acceptsAll(asList("d", "gen-ddl"));
        OptionSpec genPackageOption = parser.acceptsAll(asList("p", "gen-package"));
        OptionSpec backportOption = parser.acceptsAll(asList("b", "backport"));
        OptionSpec allOption = parser.acceptsAll(asList("a", "all"));
        OptionSpec noColorOption = parser.acceptsAll(asList("n", "no-color"));
        OptionSpec omitChangedOption = parser.acceptsAll(asList("o", "omit-changed"));
        OptionSpec ignoreErrorsOption = parser.acceptsAll(asList("i", "ignore-errors"));

        OptionSet options = parser.parse(args);

        if(!options.has(ownerSchemaOption) || !options.has(scriptsDirectoryOption)){
            throw new IllegalArgumentException("--owner-schema and --scripts-dir are required parameters.");
        }

        credentials.put(OWNER, DbCnnCredentials.create(options.valueOf(ownerSchemaOption)));
        createCredentials(USER, options, userSchemaOption);
        createCredentials(RPT, options, rptSchemaOption);
        createCredentials(PKG, options, pkgSchemaOption);
        createCredentials(PERFSTAT, options, perfstatSchemaOption);

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
        if (options.has(genPackageOption)) {
            packageDirectory = new File(scriptsDirectory.getParentFile().getAbsolutePath() + File.separator +
                  DDL_DIRECTORY_NAME + File.separator + PACKAGES_DIRECTORY_NAME);
            if (!packageDirectory.exists() || !packageDirectory.isDirectory()) {
                throw new IllegalArgumentException("Path [" + packageDirectory.getAbsolutePath() + "] doesn't exists or isn't a directory." +
                        " Can't find packages directory");
            }
        }

        if (options.has(execOption) && options.has(genDdlOption)) {
            throw new IllegalArgumentException("You can't specify both --gen-ddl and --exec arguments. Choose one.");
        }
        executeScripts = options.has(execOption);
        genDdl = options.has(genDdlOption);
        genPackage = options.has(genPackageOption);
        all = options.has(allOption);
        backport = options.has(backportOption);
        useColorLogging = !options.has(noColorOption);
        omitChanged = options.has(omitChangedOption);
        ignoreErrors = options.has(ignoreErrorsOption);
    }

    private void createCredentials(SchemaType schemaType, OptionSet options, OptionSpec<String> schemaOption) {
        if (options.hasArgument(schemaOption)) {
            String optionValue = options.valueOf(schemaOption);
            if (DbCnnCredentials.isCorrectSchemaCredentials(optionValue)) {
                String ownerConnectionString = credentials.get(OWNER).getConnectionString();
                credentials.put(schemaType, DbCnnCredentials.create(
                        DbCnnCredentials.genCnnStrForSchema(ownerConnectionString, optionValue)));
            } else {
                credentials.put(schemaType, DbCnnCredentials.create(optionValue));
            }
        } else {
            String ownerConnectionString = credentials.get(OWNER).getConnectionString();
            credentials.put(schemaType, DbCnnCredentials.create(DbCnnCredentials.genCnnStrForSchema(ownerConnectionString,
                    schemaType)));
        }
    }

    public File getScriptsDirectory() {
        return scriptsDirectory;
    }

    public File getDdlsDirectory() {
        return ddlsDirectory;
    }

    public DbCnnCredentials getDbCredentials(SchemaType schemaType) {
        return credentials.get(schemaType);
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

    public boolean isOmitChanged() {
        return omitChanged;
    }

    public boolean isIgnoreErrors() {
        return ignoreErrors;
    }

    public boolean isReadAllFilesContent() {
        return genDdl || !omitChanged;
    }

    public boolean isGenPackage() {
        return genPackage;
    }

    public File getPackageDirectory() {
        return packageDirectory;
    }

    public boolean isBackport() {
        return backport;
    }
}
