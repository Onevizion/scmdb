package com.onevizion.scmdb;

import com.onevizion.scmdb.vo.DbCnnCredentials;
import com.onevizion.scmdb.vo.SchemaType;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import oracle.ucp.jdbc.PoolDataSource;

import java.io.File;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import static com.onevizion.scmdb.vo.SchemaType.*;
import static java.util.Arrays.asList;

public class AppArguments {
    private File scriptsDirectory;
    private File ddlsDirectory;
    private File jsonSchemasDirectory;
    private File componentStructuresDirectory;
    private Map<SchemaType, DbCnnCredentials> credentials = new HashMap<>();
    private boolean genDdl;
    private boolean executeScripts;
    private boolean useColorLogging = true;
    private boolean all = false;
    private boolean omitChanged = false;
    private boolean ignoreErrors = false;
    private boolean forceDisableJobs = false;
    private boolean backport = false;
    private boolean genAllSchemas = false;
    private String ghToken;

    private final static String DDL_DIRECTORY_NAME = "ddl";
    private final static String JSON_SCHEMAS_DIRECTORY_NAME = "json";
    private final static String COMPONENT_STRUCTURES_DIRECTORY_NAME = "json-component-structures";

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
        OptionSpec allOption = parser.acceptsAll(asList("a", "all"));
        OptionSpec genAllSchemasOption = parser.accepts("gen-all-schemas");
        OptionSpec noColorOption = parser.acceptsAll(asList("n", "no-color"));
        OptionSpec omitChangedOption = parser.acceptsAll(asList("o", "omit-changed"));
        OptionSpec ignoreErrorsOption = parser.acceptsAll(asList("i", "ignore-errors"));
        OptionSpec forceDisableJobsOption = parser.accepts("force-disable-jobs");
        OptionSpec backportOption = parser.accepts("backport");
        OptionSpec<String> ghTokenOption = parser.accepts("gh-token").withRequiredArg().ofType(String.class);

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
        if(options.has(genDdlOption) || options.has(genAllSchemasOption) || options.has(backportOption)){
            ddlsDirectory = new File(scriptsDirectory.getParentFile().getAbsolutePath() + File.separator +
                    DDL_DIRECTORY_NAME);
            if (!ddlsDirectory.exists() || !ddlsDirectory.isDirectory()) {
                throw new IllegalArgumentException("Path [" + ddlsDirectory.getAbsolutePath() + "] doesn't exists or isn't a directory." +
                        " Can't find ddl directory");
            }
            jsonSchemasDirectory = new File(scriptsDirectory.getParentFile().getAbsolutePath() + File.separator +
                                            JSON_SCHEMAS_DIRECTORY_NAME);
            componentStructuresDirectory = new File(scriptsDirectory.getParentFile().getAbsolutePath() + File.separator +
                                                    COMPONENT_STRUCTURES_DIRECTORY_NAME);
            if (!jsonSchemasDirectory.exists() && !jsonSchemasDirectory.mkdirs()) {
                throw new IllegalArgumentException("Can't create json schemas directory [" +
                                                   jsonSchemasDirectory.getAbsolutePath() + "]");
            }
            if (!jsonSchemasDirectory.isDirectory()) {
                throw new IllegalArgumentException("Path [" + jsonSchemasDirectory.getAbsolutePath() +
                                                   "] isn't a directory.");
            }
            if (!componentStructuresDirectory.exists() && !componentStructuresDirectory.mkdirs()) {
                throw new IllegalArgumentException("Can't create component structures directory [" +
                                                   componentStructuresDirectory.getAbsolutePath() + "]");
            }
            if (!componentStructuresDirectory.isDirectory()) {
                throw new IllegalArgumentException("Path [" + componentStructuresDirectory.getAbsolutePath() +
                                                   "] isn't a directory.");
            }
        }

        if (options.has(execOption) && (options.has(genDdlOption) || options.has(genAllSchemasOption))) {
            throw new IllegalArgumentException("You can't specify both --gen-ddl and --exec arguments. Choose one.");
        }

        if (options.has(backportOption) && (options.has(execOption) || options.has(genDdlOption) || options.has(genAllSchemasOption))) {
            throw new IllegalArgumentException("--backport cannot be combined with --exec or --gen-ddl.");
        }

        executeScripts = options.has(execOption);
        genDdl = options.has(genDdlOption);
        all = options.has(allOption);
        genAllSchemas = options.has(genAllSchemasOption);
        useColorLogging = !options.has(noColorOption);
        omitChanged = options.has(omitChangedOption);
        ignoreErrors = options.has(ignoreErrorsOption);
        forceDisableJobs = options.has(forceDisableJobsOption);

        backport = options.has(backportOption);
        if (backport) {
            String envToken = System.getenv("GITHUB_TOKEN");
            if (envToken != null && !envToken.isBlank()) {
                ghToken = envToken;
            } else if (options.has(ghTokenOption)) {
                ghToken = options.valueOf(ghTokenOption);
            } else {
                throw new IllegalArgumentException(
                        "--gh-token or GITHUB_TOKEN environment variable is required when using --backport.");
            }
        }
    }

    public void fillDataSourceCredentials(PoolDataSource poolDataSource, SchemaType schemaType) {
        DbCnnCredentials credentials = this.credentials.get(schemaType);
        try {
            poolDataSource.setUser(credentials.getSchemaName());
            poolDataSource.setPassword(credentials.getPassword());
            poolDataSource.setURL(credentials.getOracleUrl());
        } catch (SQLException e) {
            throw new RuntimeException(MessageFormat.format("Connection creation error for the schema {}",
                                                            credentials.getSchemaName()), e);
        }
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

    public File getJsonSchemasDirectory() {
        return jsonSchemasDirectory;
    }

    public File getComponentStructuresDirectory() {
        return componentStructuresDirectory;
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
        return genDdl || backport || !omitChanged;
    }

    public boolean isForceDisableJobs() {
        return forceDisableJobs;
    }

    public boolean isBackport() {
        return backport;
    }

    public boolean isGenAllSchemas() {
        return genAllSchemas;
    }

    public String getGhToken() {
        return ghToken;
    }

}
