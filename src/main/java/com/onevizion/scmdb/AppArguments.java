package com.onevizion.scmdb;

import com.onevizion.scmdb.vo.DbCnnCredentials;
import com.onevizion.scmdb.vo.RollbackMode;
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
    private final Map<SchemaType, DbCnnCredentials> credentials = new HashMap<>();
    private boolean genDdl;
    private boolean executeScripts;
    private boolean useColorLogging = true;
    private boolean all = false;
    private boolean omitChanged = false;
    private boolean ignoreErrors = false;
    private boolean forceDisableJobs = false;
    private boolean backport = false;
    private RollbackMode rollbackMode;
    private boolean genCompsSchema = false;
    private String ghToken;

    private final static String DDL_DIRECTORY_NAME = "ddl";
    private final static String JSON_SCHEMAS_DIRECTORY_NAME = "comp-schema-tables";
    private final static String COMPONENT_STRUCTURES_DIRECTORY_NAME = "comp-schema-structure";

    private final static String DDL_DIRECTORY_NOT_FOUND_MSG =
            "Path [{0}] does not exist or is not a directory. Cannot find ddl directory";
    private final static String CANNOT_CREATE_DIRECTORY_MSG = "Cannot create {0} directory [{1}]";
    private final static String NOT_A_DIRECTORY_MSG = "Path [{0}] is not a directory";

    void parse(String[] args, boolean requireScriptsDirectory) {
        OptionParser parser = new OptionParser();
        OptionSpec<String> ownerSchemaOption = parser.accepts("owner-schema").withRequiredArg().ofType(String.class);
        OptionSpec<String> userSchemaOption = parser.accepts("user-schema").withOptionalArg().ofType(String.class);
        OptionSpec<String> rptSchemaOption = parser.accepts("rpt-schema").withOptionalArg().ofType(String.class);
        OptionSpec<String> pkgSchemaOption = parser.accepts("pkg-schema").withOptionalArg().ofType(String.class);
        OptionSpec<String> perfstatSchemaOption = parser.accepts("perfstat-schema")
                                                        .withOptionalArg()
                                                        .ofType(String.class);
        OptionSpec<File> scriptsDirectoryOption = parser.accepts("scripts-dir").withRequiredArg().ofType(File.class);

        OptionSpec<Void> execOption = parser.acceptsAll(asList("e", "exec"));
        OptionSpec<Void> genDdlOption = parser.acceptsAll(asList("d", "gen-ddl"));
        OptionSpec<Void> allOption = parser.acceptsAll(asList("a", "all"));
        OptionSpec<Void> genCompsSchemaOption = parser.accepts("gen-comps-schema");
        OptionSpec<Void> noColorOption = parser.acceptsAll(asList("n", "no-color"));
        OptionSpec<Void> omitChangedOption = parser.acceptsAll(asList("o", "omit-changed"));
        OptionSpec<Void> ignoreErrorsOption = parser.acceptsAll(asList("i", "ignore-errors"));
        OptionSpec<Void> forceDisableJobsOption = parser.accepts("force-disable-jobs");
        OptionSpec<Void> backportOption = parser.accepts("backport");
        OptionSpec<RollbackMode> rollbackMode = parser.accepts("rollback-mode")
                                                      .withRequiredArg()
                                                      .ofType(RollbackMode.class)
                                                      .defaultsTo(RollbackMode.ASK);
        OptionSpec<String> ghTokenOption = parser.accepts("gh-token").withRequiredArg().ofType(String.class);

        OptionSet options = parser.parse(args);

        if (!options.has(ownerSchemaOption)) {
            throw new IllegalArgumentException("--owner-schema is required parameter.");
        }
        if (requireScriptsDirectory && !options.has(scriptsDirectoryOption)) {
            throw new IllegalArgumentException("--scripts-dir is required parameter.");
        }

        credentials.put(OWNER, DbCnnCredentials.create(options.valueOf(ownerSchemaOption)));
        createCredentials(USER, options, userSchemaOption);
        createCredentials(RPT, options, rptSchemaOption);
        createCredentials(PKG, options, pkgSchemaOption);
        createCredentials(PERFSTAT, options, perfstatSchemaOption);

        scriptsDirectory = options.valueOf(scriptsDirectoryOption);
        boolean requireSourceTreeDirectories = options.has(genDdlOption)
                                               || options.has(genCompsSchemaOption)
                                               || options.has(backportOption);

        if (requireSourceTreeDirectories && scriptsDirectory == null) {
            throw new IllegalArgumentException(
                    "--scripts-dir is required for --gen-ddl, --gen-comps-schema and --backport modes.");
        }

        if ((requireScriptsDirectory || requireSourceTreeDirectories) &&
            (!scriptsDirectory.exists() || !scriptsDirectory.isDirectory())) {
            throw new IllegalArgumentException(
                    "Path [" + scriptsDirectory.getAbsolutePath() + "] doesn't exists or isn't a directory." +
                    " [--scripts-dir] should contains absolute path and points to scripts directory");
        } else if (!requireScriptsDirectory && scriptsDirectory != null && !requireSourceTreeDirectories) {
            throw new IllegalArgumentException(
                    "[--scripts-dir] parameter is not expected in this context, SCMDB has already migration scripts bundled.");
        }

        if (requireSourceTreeDirectories) {
            resolveSourceTreeDirectories();
        }

        if (options.has(execOption) && (options.has(genDdlOption) || options.has(genCompsSchemaOption))) {
            throw new IllegalArgumentException("You can't specify --exec together with --gen-ddl or --gen-comps-schema. Choose one mode.");
        }

        if (options.has(backportOption) &&
            (options.has(execOption) || options.has(genDdlOption) || options.has(genCompsSchemaOption))) {
            throw new IllegalArgumentException("--backport cannot be combined with --exec, --gen-ddl or --gen-comps-schema.");
        }

        executeScripts = options.has(execOption);
        genDdl = options.has(genDdlOption);
        all = options.has(allOption);
        genCompsSchema = options.has(genCompsSchemaOption);
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

        this.rollbackMode = options.valueOf(rollbackMode);
    }

    private void resolveSourceTreeDirectories() {
        File dbDirectory = scriptsDirectory.getAbsoluteFile().getParentFile();
        ddlsDirectory = new File(dbDirectory, DDL_DIRECTORY_NAME);
        if (!ddlsDirectory.exists() || !ddlsDirectory.isDirectory()) {
            throw new IllegalArgumentException(MessageFormat.format(DDL_DIRECTORY_NOT_FOUND_MSG,
                                                                    ddlsDirectory.getAbsolutePath()));
        }
        jsonSchemasDirectory = ensureDirectory(dbDirectory, JSON_SCHEMAS_DIRECTORY_NAME, "component schema tables");
        componentStructuresDirectory = ensureDirectory(dbDirectory, COMPONENT_STRUCTURES_DIRECTORY_NAME,
                                                       "component schema structure");
    }

    private File ensureDirectory(File parentDirectory, String directoryName, String description) {
        File directory = new File(parentDirectory, directoryName);
        if (!directory.exists() && !directory.mkdirs()) {
            throw new IllegalArgumentException(MessageFormat.format(CANNOT_CREATE_DIRECTORY_MSG,
                                                                    description, directory.getAbsolutePath()));
        }
        if (!directory.isDirectory()) {
            throw new IllegalArgumentException(MessageFormat.format(NOT_A_DIRECTORY_MSG, directory.getAbsolutePath()));
        }
        return directory;
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
            credentials.put(schemaType,
                            DbCnnCredentials.create(DbCnnCredentials.genCnnStrForSchema(ownerConnectionString,
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
        return genDdl || genCompsSchema || backport || !omitChanged;
    }

    public boolean isForceDisableJobs() {
        return forceDisableJobs;
    }

    public boolean isBackport() {
        return backport;
    }

    public RollbackMode getRollbackMode() {
        return rollbackMode;
    }

    public boolean isGenCompsSchema() {
        return genCompsSchema;
    }

    public String getGhToken() {
        return ghToken;
    }

}
