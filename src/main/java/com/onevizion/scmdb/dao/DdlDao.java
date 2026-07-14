package com.onevizion.scmdb.dao;

import com.onevizion.scmdb.StringPlaceholderUtils;
import com.onevizion.scmdb.vo.DbObject;
import com.onevizion.scmdb.vo.DbObjectType;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import static com.onevizion.scmdb.vo.DbObjectType.*;

@Component
public class DdlDao extends AbstractDaoOra {

    private final static String TABLE_NAME_COLUMN_NAME = "table_name";
    private final static String DDL_COLUMN_NAME = "ddl";
    private final static String COMPRESSION_COLUMN_NAME = "compression";
    private final static String PREFIX_LENGTH_COLUMN_NAME = "prefix_length";
    private final static String FIND_ALL_DB_OBJECTS = "select object_name,\n" +
            "       object_type\n" +
            "from user_objects\n" +
            "where (object_type = 'TABLE'\n" +
            "    and generated = 'N'\n" +
            "    and object_name not like 'Z_%'" +
            "    and object_name not like '%_OLD')\n" +
            "   or object_type = 'VIEW'\n" +
            "   or object_type = 'PACKAGE'\n" +
            "   or object_type = 'PACKAGE BODY'\n" +
            "   or object_type = 'TRIGGER'\n" +
            "   or ((object_type = 'TYPE' or object_type = 'TYPE BODY')\n" +
            "     and generated = 'N'\n" +
            "     and object_name not like 'T$%')\n";

    private final static String SELECT_DDL_COMMENTS_BY_TABLE_NAME = "select table_name, dbms_metadata.get_dependent_ddl('COMMENT', table_name) from" +
            " ((select table_name from user_tab_comments" +
            "     where comments is not null)" +
            " union" +
            "  (select table_name from user_col_comments" +
            "     where comments is not null" +
            "     group by table_name)) where table_name = upper(:tableName)";

    private final static String SELECT_DDL_SEQUENCE_BY_TABLE_NAME = "select trgrs.table_name, dbms_metadata.get_ddl('SEQUENCE', depends.referenced_name)" +
            " from user_dependencies depends, user_triggers trgrs" +
            " where trgrs.trigger_name = depends.name and depends.type = 'TRIGGER'" +
            " and depends.referenced_type = 'SEQUENCE' and trgrs.table_name = upper(:tableName)" +
            " order by depends.referenced_name";

    private final static String SELECT_DDL_INDEX_BY_TABLE_NAME = "select table_name, dbms_metadata.get_ddl('INDEX', index_name) as ddl," +
            " case when (compression = 'ENABLED') then 1 else 0 end as compression, prefix_length" +
            " from user_indexes where generated = 'N' and table_name=upper(:tableName) and index_name not like 'PK_%'" +
            " order by table_name asc, uniqueness desc, regexp_substr(index_name, '^\\D*') nulls first, " +
            "  to_number(regexp_substr(index_name, '\\d+'))";

    private final static String SELECT_DDL_TRIGGER_BY_TABLE_NAME = "select table_name, dbms_metadata.get_ddl('TRIGGER', trigger_name)" +
            " from user_triggers where table_name=upper(:tableName)" +
            " and trigger_name not like 'Z_%' order by nlssort(trigger_name, 'NLS_SORT = BINARY_CI')";

    private final static RowMapper<DbObject> rowMapper = (rs, rowNum) -> {
        DbObject dbObject = new DbObject();
        dbObject.setName(rs.getString(1));
        dbObject.setDdl(rs.getString(2));
        return dbObject;
    };

    private final static RowMapper<DbObject> indexRowMapper = (rs, rowNum) -> {
        DbObject dbObject = new DbObject();
        dbObject.setName(rs.getString(TABLE_NAME_COLUMN_NAME));
        String replacement;
        if (rs.getBoolean(COMPRESSION_COLUMN_NAME)) {
            int compressLength = rs.getInt(PREFIX_LENGTH_COLUMN_NAME);
            replacement = String.format(" COMPRESS %d;", compressLength);
        } else {
            replacement = ";";
        }
        dbObject.setDdl(rs.getString(DDL_COLUMN_NAME).replaceAll("\\s+;$", replacement));
        return dbObject;
    };

    private final static RowMapper<DbObject> rowMapperWithObjectType = (rs, rowNum) -> {
        DbObject dbObject = new DbObject();
        dbObject.setName(rs.getString("object_name"));
        dbObject.setType(DbObjectType.getByName(rs.getString("object_type")));
        return dbObject;
    };
    
    public void executeTransformParamStatements() {
        String plsqlBlock = "begin" +
                "\n dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',true);" +
                "\n dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',true);" +
                "\n dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SEGMENT_ATTRIBUTES',false);" +
                "\n end;";
        jdbcTemplate.execute(plsqlBlock);
    }

    public List<DbObject> extractAllDbObjectsWithoutDdl() {
        return jdbcTemplate.query(FIND_ALL_DB_OBJECTS, rowMapperWithObjectType);
    }

    public String extractDdl(DbObject dbObject) {
        String sql = "select dbms_metadata.get_ddl(upper(:dbObjType), upper(:dbObjName)) from dual";
        MapSqlParameterSource namedParams = new MapSqlParameterSource();
        namedParams.addValue("dbObjName", dbObject.getName());
        namedParams.addValue("dbObjType", dbObject.getType().toString());
        return namedParameterJdbcTemplate.queryForObject(sql, namedParams, String.class);
    }

    public List<DbObject> extractTableDependentObjectsDdl(String tableName, DbObjectType depObjType) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource("tableName", tableName);

        List<DbObject> dbObjects = null;
        if (depObjType == COMMENT) {
            dbObjects = namedParameterJdbcTemplate.query(SELECT_DDL_COMMENTS_BY_TABLE_NAME, namedParams, rowMapper);
        } else if (depObjType == SEQUENCE) {
            dbObjects = namedParameterJdbcTemplate.query(SELECT_DDL_SEQUENCE_BY_TABLE_NAME, namedParams, rowMapper);
        } else if (depObjType == INDEX) {
            dbObjects = namedParameterJdbcTemplate.query(SELECT_DDL_INDEX_BY_TABLE_NAME, namedParams, indexRowMapper);
        } else if (depObjType == TRIGGER) {
            dbObjects = namedParameterJdbcTemplate.query(SELECT_DDL_TRIGGER_BY_TABLE_NAME, namedParams, rowMapper);
        }

        return dbObjects;
    }

    public String getTableNameByDepObject(DbObject dbObject) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource();
        namedParams.addValue("objName", dbObject.getName());
        String sql;
        if (dbObject.getType() == INDEX) {
            sql = "select table_name from user_indexes where index_name = upper(:objName)";
        } else if (dbObject.getType() == TRIGGER) {
            sql = "select table_name from user_triggers where trigger_name = upper(:objName)";
        } else if (dbObject.getType() == SEQUENCE) {
            sql = "select trgrs.table_name from user_dependencies depends, user_triggers trgrs" +
                    " where trgrs.trigger_name = depends.name and depends.type = 'TRIGGER'" +
                    " and depends.referenced_type = 'SEQUENCE' and depends.referenced_name = upper(:objName)";
        } else {
            return null;
        }
        try {
            return namedParameterJdbcTemplate.queryForObject(sql, namedParams, String.class);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public DbObjectType getObjectTypeByName(String dbObjName) {
        String sql = "select 'TABLE' object_type from user_tables where table_name = upper(:dbObjName)" +
                " union all select 'VIEW' object_type from user_views where view_name = upper(:dbObjName)";
        MapSqlParameterSource namedParams = new MapSqlParameterSource();
        namedParams.addValue("dbObjName", dbObjName);
        String dbObjTypeString = namedParameterJdbcTemplate.queryForObject(sql, namedParams, String.class);
        return DbObjectType.valueOf(dbObjTypeString);
    }

    public boolean isExist(String objectName, DbObjectType objectType) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource();
        namedParams.addValue("objName", objectName);
        namedParams.addValue("objType", objectType.getName().toUpperCase());
        String sql = """
                select case when count(object_name) > 0 then 'true'
                            else 'false'
                        end
                    from user_objects where object_name = upper(:objName) and object_type = upper(:objType)
                """;
        String boolStr = namedParameterJdbcTemplate.queryForObject(sql, namedParams, String.class);
        return Boolean.valueOf(boolStr);
    }

    public boolean isStaticReferenceTable(String tableName) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource("tableName", tableName);
        String sql = """
                select case when count(*) = 1 then 'true' else 'false' end
                  from user_tables t
                 where t.table_name = upper(:tableName)
                   and not exists (select 1 from user_tab_columns c
                                    where c.table_name = t.table_name and c.column_name = 'PROGRAM_ID')
                """;
        String boolStr = namedParameterJdbcTemplate.queryForObject(sql, namedParams, String.class);
        return Boolean.valueOf(boolStr);
    }

    public List<String> getPrimaryKeyColumns(String tableName) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource("tableName", tableName);
        String sql = """
                select cols.column_name
                  from user_constraints cons
                  join user_cons_columns cols on cols.constraint_name = cons.constraint_name
                 where cons.table_name = upper(:tableName)
                   and cons.constraint_type = 'P'
                 order by cols.position
                """;
        return namedParameterJdbcTemplate.queryForList(sql, namedParams, String.class);
    }

    public List<String> getLookupColumns(String tableName, List<String> excludedColumns) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource("tableName", tableName);
        String sql = """
                select column_name
                  from user_tab_columns
                 where table_name = upper(:tableName)
                   and data_type in ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR')
                 order by case when column_name like '%NAME%' then 0
                               when column_name like '%CODE%' then 1
                               when column_name like '%TYPE%' then 2
                               else 3
                           end,
                           column_id
                """;
        return namedParameterJdbcTemplate.queryForList(sql, namedParams, String.class)
                                         .stream()
                                         .filter(columnName -> !excludedColumns.contains(columnName))
                                         .toList();
    }

    public List<Map<String, Object>> loadReferenceData(String tableName, String pkColumn, String lookupColumn) {
        tableName = sanitizeSqlIdentifier(tableName);
        pkColumn = sanitizeSqlIdentifier(pkColumn);
        lookupColumn = sanitizeSqlIdentifier(lookupColumn);

        String whereClause = referenceDataWhereClause(tableName);
        String sql = StringPlaceholderUtils.replace("""
                                                    select {pkColumn} as id,
                                                           {lookupColumn} as name
                                                      from {tableName}{whereClause}
                                                     order by {pkColumn}""",
                                                    Map.of("pkColumn", pkColumn,
                                                           "lookupColumn", lookupColumn,
                                                           "tableName", tableName,
                                                           "whereClause", whereClause));
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", rs.getObject("id"));
            row.put("name", rs.getObject("name"));
            return row;
        });
    }

    public List<Map<String, Object>> loadComponentStructureRows() {
        boolean hasBpdItemTypeId = hasColumn("V_COMPONENT", "BPD_ITEM_TYPE_ID");
        String bpdItemTypeIdColumn = hasBpdItemTypeId ? "c.bpd_item_type_id,"
                                                      : "cast(null as number) as bpd_item_type_id,";
        String bpdItemTypeColumn = hasBpdItemTypeId ? "bit.item_type as bpd_item_type"
                                                    : "cast(null as varchar2(4000)) as bpd_item_type";
        String bpdItemTypeJoin = hasBpdItemTypeId ? "left join bpd_item_type bit on bit.item_type_id = c.bpd_item_type_id"
                                                  : "";
        String sql = StringPlaceholderUtils.replace("""
                                                    select c.component_id,
                                                           c.component,
                                                           c.main_table,
                                                           c.support_bpl,
                                                           c.support_audit,
                                                           c.component_name_column,
                                                           t.component_table_id,
                                                           t.table_name,
                                                           {bpdItemTypeIdColumn}
                                                           {bpdItemTypeColumn}
                                                      from v_component c
                                                      left join v_component_table t on t.component_id = c.component_id
                                                      {bpdItemTypeJoin}
                                                     order by c.component, t.table_name""",
                                                    Map.of("bpdItemTypeIdColumn", bpdItemTypeIdColumn,
                                                           "bpdItemTypeColumn", bpdItemTypeColumn,
                                                           "bpdItemTypeJoin", bpdItemTypeJoin));
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("component_id", rs.getObject("component_id"));
            row.put("component", rs.getObject("component"));
            row.put("main_table", rs.getObject("main_table"));
            row.put("support_bpl", rs.getObject("support_bpl"));
            row.put("support_audit", rs.getObject("support_audit"));
            row.put("component_name_column", rs.getObject("component_name_column"));
            row.put("component_table_id", rs.getObject("component_table_id"));
            row.put("table_name", rs.getObject("table_name"));
            row.put("bpd_item_type_id", rs.getObject("bpd_item_type_id"));
            row.put("bpd_item_type", rs.getObject("bpd_item_type"));
            return row;
        });
    }

    private String referenceDataWhereClause(String tableName) {
        if (!"GRID_PAGE".equals(tableName)) {
            return "";
        }

        return """
                 where page_url is not null
                 and is_tt_specific = 1
                 and module_name not like 'ADMIN%'
                 and security_group not like 'ADMIN%'
                 and module_name not like 'BPL_EXP_IMP%'
                 and module_name not like 'SELECTOR%'
                 and module_name not like 'ASSIGNMENT%'
                """;
    }

    private boolean hasColumn(String tableName, String columnName) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource()
                .addValue("tableName", tableName)
                .addValue("columnName", columnName);
        String sql = """
                select case when count(*) > 0 then 'true' else 'false' end
                 from user_tab_columns
                 where table_name = upper(:tableName)
                   and column_name = upper(:columnName)
                """;
        String boolStr = namedParameterJdbcTemplate.queryForObject(sql, namedParams, String.class);
        return Boolean.valueOf(boolStr);
    }

    public List<String> loadComponentTableNames() {
        String sql = """
                select main_table as table_name
                 from v_component
                 where main_table is not null
                 union
                 select table_name
                 from v_component_table
                 where table_name is not null
                """;
        return jdbcTemplate.queryForList(sql, String.class);
    }

    public String getComponentLookupColumn(String tableName) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource("tableName", tableName);
        String sql = """
                select distinct component_name_column
                  from v_component
                 where main_table = upper(:tableName)
                   and component_name_column is not null
                 order by component_name_column
                """;
        List<String> lookupColumns = namedParameterJdbcTemplate.queryForList(sql, namedParams, String.class);
        return lookupColumns.isEmpty() ? null : lookupColumns.get(0);
    }

    public List<Map<String, Object>> getTableForeignKeys(String tableName) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource("tableName", tableName);
        String sql = """
                select ac.constraint_name,
                       ac.table_name as from_table,
                       acc.column_name as from_column,
                       r_ac.table_name as to_table,
                       r_acc.column_name as to_column
                  from user_constraints ac
                  join user_cons_columns acc on ac.constraint_name = acc.constraint_name
                  join user_constraints r_ac on ac.r_constraint_name = r_ac.constraint_name
                  join user_cons_columns r_acc on r_ac.constraint_name = r_acc.constraint_name
                                              and acc.position = r_acc.position
                 where ac.constraint_type = 'R'
                   and ac.table_name = upper(:tableName)
                 order by ac.constraint_name, acc.position
                """;
        return namedParameterJdbcTemplate.query(sql, namedParams, (rs, rowNum) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("constraint_name", rs.getObject("constraint_name"));
            row.put("from_table", rs.getObject("from_table"));
            row.put("from_column", rs.getObject("from_column"));
            row.put("to_table", rs.getObject("to_table"));
            row.put("to_column", rs.getObject("to_column"));
            return row;
        });
    }

    private String sanitizeSqlIdentifier(String identifier) {
        String sanitized = identifier.toUpperCase(Locale.ROOT);
        if (!sanitized.matches("[A-Z][A-Z0-9_$#]*")) {
            throw new IllegalArgumentException("Unsafe SQL identifier: " + identifier);
        }
        return sanitized;
    }

}
