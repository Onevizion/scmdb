package com.onevizion.scmdb;

import com.onevizion.scmdb.dao.DdlDao;
import com.onevizion.scmdb.model.ReferenceKind;
import com.onevizion.scmdb.model.ReferenceMetadata;
import com.onevizion.scmdb.model.StaticValueMetadata;
import com.onevizion.scmdb.model.TableMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

@Component
public class DdlTableMetadataProvider {
    private final AppArguments appArguments;
    private final DdlDao ddlDao;
    private final ColorLogger logger;
    private final OracleDdlParser parser;

    @Autowired
    public DdlTableMetadataProvider(AppArguments appArguments, DdlDao ddlDao, ColorLogger logger) {
        this(appArguments, ddlDao, logger, new OracleDdlParser());
    }

    DdlTableMetadataProvider(AppArguments appArguments, DdlDao ddlDao, ColorLogger logger, OracleDdlParser parser) {
        this.appArguments = appArguments;
        this.ddlDao = ddlDao;
        this.logger = logger;
        this.parser = parser;
    }

    public TableMetadata load(String tableName) {
        Path file = appArguments.getDdlsDirectory().toPath().resolve("tables")
                                .resolve(tableName.toLowerCase(Locale.ROOT) + ".sql");
        if (!Files.isRegularFile(file)) {
            return null;
        }

        try {
            return enrich(parser.parse(Files.readString(file)));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read table DDL: " + file.toAbsolutePath(), e);
        }
    }

    TableMetadata parse(String ddl) {
        return parser.parse(ddl);
    }

    private TableMetadata enrich(TableMetadata table) {
        List<String> primaryKey = resolvePrimaryKey(table);
        ReferenceMetadata referenceData = null;

        if (primaryKey.size() == 1) {
            boolean staticReference = ddlDao.isStaticReferenceTableByName(table.name());
            String lookupColumn = staticReference
                                    ? ddlDao.findLookupColumnNamesByTableName(table.name(), primaryKey)
                                            .stream()
                                            .findFirst().orElse(null)
                                    : ddlDao.getComponentLookupColumn(table.name());
            if (lookupColumn != null && table.column(lookupColumn) != null) {
                List<StaticValueMetadata> values = staticReference
                                                 ? ddlDao.getTableData(table.name(), primaryKey.get(0), lookupColumn)
                                                 : List.of();
                referenceData = new ReferenceMetadata(
                        staticReference ? ReferenceKind.STATIC : ReferenceKind.DYNAMIC,
                        table.name(), primaryKey, List.of(lookupColumn), values);
            }
        }

        return LabelReferenceConventions.apply(new TableMetadata(table.name(), table.description(), table.columns(),
                primaryKey, table.foreignKeys(), table.checks(), referenceData));
    }

    private List<String> resolvePrimaryKey(TableMetadata table) {
        List<String> ddlPrimaryKey = table.primaryKey();
        List<String> dbPrimaryKey = ddlDao.findPrimaryKeyColumnNamesByTableName(table.name());
        if (ddlPrimaryKey.isEmpty()) {
            return dbPrimaryKey;
        }
        if (!dbPrimaryKey.isEmpty() && !new LinkedHashSet<>(ddlPrimaryKey).equals(new LinkedHashSet<>(dbPrimaryKey))) {
            logger.warn("Primary key mismatch for table [{}]: DDL declares {}, database reports {}. Using DDL-derived primary key.",
                    ColorLogger.Color.YELLOW, table.name(), ddlPrimaryKey, dbPrimaryKey);
        }
        return ddlPrimaryKey;
    }
}
