package com.onevizion.scmdb.dao;

import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;

import com.onevizion.scmdb.StringPlaceholderUtils;
import com.onevizion.scmdb.vo.ComponentRow;
import com.onevizion.scmdb.vo.DbObject;
import com.onevizion.scmdb.vo.DbObjectType;
import com.onevizion.scmdb.vo.ForeignKey;

@Component
public class DdlDao extends AbstractDaoOra {

    private static final String TABLE_NAME = "tableName";
    private static final String COLUMN_NAME = "columnName";

    private final static String FIND_ALL_DB_OBJECTS = """
            select object_name as name,
                   object_type as type,
                   null as ddl
              from user_objects
             where (object_type = 'TABLE'
                    and generated = 'N'
                    and object_name not like 'Z_%'
                    and object_name not like '%_OLD')
                or object_type = 'VIEW'
                or object_type = 'PACKAGE'
                or object_type = 'PACKAGE BODY'
                or object_type = 'TRIGGER'
                or ((object_type = 'TYPE' or object_type = 'TYPE BODY')
                    and generated = 'N'
                    and object_name not like 'T$%')
            """;

    private final static String FIND_DDL_COMMENTS_BY_TABLE_NAME = """
            select table_name as name,
                   'COMMENT' as type,
                   dbms_metadata.get_dependent_ddl('COMMENT', table_name) as ddl
              from (select table_name
                      from user_tab_comments
                     where comments is not null
                     union
                    select table_name
                      from user_col_comments
                     where comments is not null
                     group by table_name)
              where table_name = upper(:tableName)
            """;

    private final static String FIND_DDL_SEQUENCE_BY_TABLE_NAME = """
            select trgrs.table_name as name,
                   'SEQUENCE' as type,
                   dbms_metadata.get_ddl('SEQUENCE', depends.referenced_name) as ddl
              from user_dependencies depends,
                   user_triggers trgrs
             where trgrs.trigger_name = depends.name
               and depends.type = 'TRIGGER'
               and depends.referenced_type = 'SEQUENCE'
               and trgrs.table_name = upper(:tableName)
             order by depends.referenced_name
            """;

    private final static String FIND_DDL_INDEX_BY_TABLE_NAME = """
            select table_name as name,
                   'INDEX' as type,
                   dbms_metadata.get_ddl('INDEX', index_name) as ddl,
                   case when (compression = 'ENABLED') then 1 else 0 end as compression,
                   prefix_length
              from user_indexes
             where generated = 'N'
               and table_name=upper(:tableName)
               and index_name not like 'PK_%'
             order by table_name asc,
                      uniqueness desc,
                      regexp_substr(index_name, '^\\D*') nulls first,
                      to_number(regexp_substr(index_name, '\\d+'))
            """;

    private final static String FIND_DDL_TRIGGER_BY_TABLE_NAME = """
            select table_name as name,
                   'TRIGGER' as type,
                   dbms_metadata.get_ddl('TRIGGER', trigger_name) as ddl
              from user_triggers
             where table_name = upper(:tableName)
               and trigger_name not like 'Z_%'
             order by nlssort(trigger_name, 'NLS_SORT = BINARY_CI')
            """;

    private static final String EXECUTE_TRANSFORM_PARAMS = """
            begin
                dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'PRETTY', true);
                dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'SQLTERMINATOR', true);
                dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'SEGMENT_ATTRIBUTES', false);
            end;
            """;

    private static final String READ_TABLE_NAME_BY_INDEX_NAME = """
            select table_name
              from user_indexes
             where index_name = upper(:name)
            """;

    private static final String READ_TABLE_NAME_BY_TRIGGER_NAME = """
            select table_name 
              from user_triggers
             where trigger_name = upper(:name)
            """;

    private static final String READ_TABLE_NAME_BY_SEQUENCE_NAME = """
            select trgrs.table_name
            from user_dependencies depends,
                 user_triggers trgrs
           where trgrs.trigger_name = depends.name
             and depends.type = 'TRIGGER'
             and depends.referenced_type = 'SEQUENCE'
             and depends.referenced_name = upper(:name)
          """;

    private static final String READ_DDL = "select dbms_metadata.get_ddl(upper(:type), upper(:name)) from dual";

    private static final String READ_OBJECT_TYPE_BY_NAME = """
            select 'TABLE' object_type
              from user_tables
             where table_name = upper(:dbObjName)
             union all
            select 'VIEW' object_type
              from user_views
             where view_name = upper(:dbObjName)
            """;

    private static final String COUNT_OBJECTS_BY_NAME_AND_TYPE = """
            select count(object_name)
              from user_objects
             where object_name = upper(:name)
               and object_type = upper(:type.name)
            """;

    private static final String COUNT_STATIC_TABLES_BY_NAME = """
            select count(1)
              from user_tables t
             where t.table_name = upper(:tableName)
               and not exists (select 1
                                 from user_tab_columns c
                                where c.table_name = t.table_name
                                  and c.column_name = 'PROGRAM_ID')
            """;

    private static final String FIND_PRIMARY_KEY_COLUMNS_BY_TABLE_NAME = """
            select cols.column_name
              from user_constraints cons
              join user_cons_columns cols on cols.constraint_name = cons.constraint_name
             where cons.table_name = upper(:tableName)
               and cons.constraint_type = 'P'
             order by cols.position
            """;

    private static final String FIND_LOOKUP_COLUMN_NAMES_BY_TABLE_NAME = """
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

    private static final String FIND_FOREIGN_KEYS_BY_TABLE_NAME = """
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

    private static final String FIND_ALL_COMPONENT_MAIN_TABLE_NAMES = """
            select main_table as table_name
              from v_component
             where main_table is not null
             union
            select table_name
              from v_component_table
             where table_name is not null
           """;

    private static final String GET_COMPONENT_LOOKUP_COLUMN_BY_TABLE_NAME = """
            select distinct component_name_column
              from v_component
             where main_table = upper(:tableName)
               and component_name_column is not null
             order by component_name_column
            """;

    private static final String HAS_COLUMN = """
            select count(1)
              from user_tab_columns
             where table_name = upper(:tableName)
               and column_name = upper(:columnName)
            """;

    private static final String FIND_COMPONENT_ROWS = """
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
             order by c.component, t.table_name
            """;

    private static final String READ_TABLE_DATA = """
            select {pkColumn} as id,
                   {lookupColumn} as name
              from {tableName}
              {whereClause}
             order by {pkColumn}
            """;

    private static final RowMapper<Map<String, Object>> readTableDataRowMapper = (rs, rowNum) -> {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", rs.getObject("id"));
        row.put("name", rs.getObject("name"));
        return row;
    };

    private final static RowMapper<DbObject> dbObjectRowMapper = (rs, rowNum) -> {
        DbObject dbObject = new DbObject();
        dbObject.setName(StringUtils.defaultIfBlank(rs.getString("name"), null));
        dbObject.setDdl(StringUtils.defaultIfBlank(rs.getString("ddl"), null));

        Optional.ofNullable(rs.getString("type"))
                .filter(StringUtils::isNotBlank)
                .map(DbObjectType::getByName)
                .ifPresent(dbObject::setType);

        return dbObject;
    };

    private final static RowMapper<DbObject> indexDbObjectRowMapper = (rs, rowNum) -> {
        DbObject dbObject = dbObjectRowMapper.mapRow(rs, rowNum);

        String replacement;
        if (rs.getBoolean("compression")) {
            int compressLength = rs.getInt("prefix_length");
            replacement = String.format(" COMPRESS %d;", compressLength);
        } else {
            replacement = ";";
        }
        dbObject.setDdl(dbObject.getDdl().replaceAll("\\s+;$", replacement));

        return dbObject;
    };

    private static final RowMapper<ForeignKey> foreignKeyRowMapper = (rs, rowNum) ->
            new ForeignKey(rs.getString("constraint_name"),
                           rs.getString("from_table"),
                           rs.getString("from_column"),
                           rs.getString("to_table"),
                           rs.getString("to_column"));

    private static final RowMapper<ComponentRow> componentRowMapper = (rs, rowNum) ->
            new ComponentRow(rs.getObject("component_id", Integer.class),
                             rs.getString("component"),
                             rs.getString("main_table"),
                             rs.getObject("support_bpl", Integer.class),
                             rs.getObject("support_audit", Integer.class),
                             rs.getString("component_name_column"),
                             rs.getObject("component_table_id", Integer.class),
                             rs.getString("table_name"),
                             rs.getObject("bpd_item_type_id", Integer.class),
                             rs.getString("bpd_item_type"));

    public void executeTransformParamStatements() {
        jdbcTemplate.execute(EXECUTE_TRANSFORM_PARAMS);
    }

    public List<DbObject> findAllDbObjects() {
        return jdbcTemplate.query(FIND_ALL_DB_OBJECTS, dbObjectRowMapper);
    }

    public String getDdl(DbObject dbObject) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("name", dbObject.getName(), Types.VARCHAR)
              .addValue("type", dbObject.getType().toString(), Types.VARCHAR);
        return namedParameterJdbcTemplate.queryForObject(READ_DDL, params, String.class);
    }

    public List<DbObject> findTableRelatedObjectDdlByTableNameAndObjectType(String tableName, DbObjectType dbOjectType) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource()
                .addValue(TABLE_NAME, tableName, Types.VARCHAR);

        return switch(dbOjectType) {
            case COMMENT -> namedParameterJdbcTemplate.query(FIND_DDL_COMMENTS_BY_TABLE_NAME, namedParams, dbObjectRowMapper);
            case SEQUENCE -> namedParameterJdbcTemplate.query(FIND_DDL_SEQUENCE_BY_TABLE_NAME, namedParams, dbObjectRowMapper);
            case INDEX -> namedParameterJdbcTemplate.query(FIND_DDL_INDEX_BY_TABLE_NAME, namedParams, indexDbObjectRowMapper);
            case TRIGGER -> namedParameterJdbcTemplate.query(FIND_DDL_TRIGGER_BY_TABLE_NAME, namedParams, dbObjectRowMapper);
            default -> List.of();
        };
    }

    public String getTableNameByDepObject(DbObject dbObject) {
        SqlParameterSource params = new BeanPropertySqlParameterSource(dbObject);

        String sql = switch(dbObject.getType()) {
            case INDEX -> READ_TABLE_NAME_BY_INDEX_NAME;
            case TRIGGER -> READ_TABLE_NAME_BY_TRIGGER_NAME;
            case SEQUENCE -> READ_TABLE_NAME_BY_SEQUENCE_NAME;
            default -> throw new IllegalArgumentException("Unsupported DB object type [%s]".formatted(dbObject.getType()));
        };

        String tableName = null;
        if (StringUtils.isNotBlank(sql)) {
            try {
                tableName = namedParameterJdbcTemplate.queryForObject(sql, params, String.class);
            } catch (EmptyResultDataAccessException ignored) {
                // Table name is not found. Ignore it silently.
            }
        }
        return tableName;
    }

    public DbObjectType getDbObjectTypeByName(String dbObjName) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource("dbObjName", dbObjName);
        String objectTypeName = namedParameterJdbcTemplate.queryForObject(READ_OBJECT_TYPE_BY_NAME, namedParams, String.class);
        return DbObjectType.getByName(objectTypeName);
    }

    public boolean isExist(DbObject dbObject) {
        SqlParameterSource params = new BeanPropertySqlParameterSource(dbObject);
        int countObjects = namedParameterJdbcTemplate.queryForObject(COUNT_OBJECTS_BY_NAME_AND_TYPE, params, int.class);
        return countObjects > 0;
    }

    public boolean isStaticReferenceTableByName(String tableName) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource()
                .addValue(TABLE_NAME, tableName, Types.VARCHAR);
        int countObjects = namedParameterJdbcTemplate.queryForObject(COUNT_STATIC_TABLES_BY_NAME, namedParams, int.class);
        return countObjects == 1;
    }

    public List<String> findPrimaryKeyColumnNamesByTableName(String tableName) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource()
                .addValue(TABLE_NAME, tableName, Types.VARCHAR);
        return namedParameterJdbcTemplate.queryForList(FIND_PRIMARY_KEY_COLUMNS_BY_TABLE_NAME, namedParams, String.class);
    }

    public List<String> findLookupColumnNamesByTableName(String tableName, List<String> excludedColumns) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource()
                .addValue(TABLE_NAME, tableName, Types.VARCHAR);
        return namedParameterJdbcTemplate.queryForList(FIND_LOOKUP_COLUMN_NAMES_BY_TABLE_NAME, namedParams, String.class)
                                         .stream()
                                         .filter(columnName -> !excludedColumns.contains(columnName))
                                         .toList();
    }

    public List<Map<String, Object>> getTableData(String tableName, String pkColumn, String lookupColumn) {
        String whereClause = null;
        if ("GRID_PAGE".equalsIgnoreCase(tableName)) {
            whereClause = """
                    where page_url is not null
                      and is_tt_specific = 1
                      and module_name not like 'ADMIN%'
                      and security_group not like 'ADMIN%'
                      and module_name not like 'BPL_EXP_IMP%'
                      and module_name not like 'SELECTOR%'
                      and module_name not like 'ASSIGNMENT%'
                    """;
        }

        Map<String, Object> placeholders = Map.of("pkColumn", sanitizeSqlIdentifier(pkColumn),
                                                  "lookupColumn", sanitizeSqlIdentifier(lookupColumn),
                                                  "tableName", sanitizeSqlIdentifier(tableName),
                                                  "whereClause", StringUtils.defaultIfBlank(whereClause, ""));
        String sql = StringPlaceholderUtils.replace(READ_TABLE_DATA, placeholders);

        return jdbcTemplate.query(sql, readTableDataRowMapper);
    }

    public List<ComponentRow> findComponentRows(boolean hasBpdItemTypeId) {
        String bpdItemTypeIdColumn = hasBpdItemTypeId ? "c.bpd_item_type_id,"
                                                      : "cast(null as number) as bpd_item_type_id,";
        String bpdItemTypeColumn = hasBpdItemTypeId ? "bit.item_type as bpd_item_type"
                                                    : "cast(null as varchar2(4000)) as bpd_item_type";
        String bpdItemTypeJoin = hasBpdItemTypeId ? "left join bpd_item_type bit on bit.item_type_id = c.bpd_item_type_id"
                                                  : "";

        Map<String, Object> placeholders = Map.of("bpdItemTypeIdColumn", bpdItemTypeIdColumn,
                                                  "bpdItemTypeColumn", bpdItemTypeColumn,
                                                  "bpdItemTypeJoin", bpdItemTypeJoin);
        String sql = StringPlaceholderUtils.replace(FIND_COMPONENT_ROWS, placeholders);

        return jdbcTemplate.query(sql, componentRowMapper);
    }

    public boolean hasColumnInTable(String tableName, String columnName) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource()
                .addValue(TABLE_NAME, tableName, Types.VARCHAR)
                .addValue(COLUMN_NAME, columnName, Types.VARCHAR);
        int countColumns = namedParameterJdbcTemplate.queryForObject(HAS_COLUMN, namedParams, int.class);
        return countColumns > 0;
    }

    public List<String> findAllComponentMainTableNames() {
        return jdbcTemplate.queryForList(FIND_ALL_COMPONENT_MAIN_TABLE_NAMES, String.class);
    }

    public String getComponentLookupColumn(String tableName) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource()
                .addValue(TABLE_NAME, tableName, Types.VARCHAR);
        List<String> lookupColumns = namedParameterJdbcTemplate.queryForList(GET_COMPONENT_LOOKUP_COLUMN_BY_TABLE_NAME, namedParams, String.class);
        return lookupColumns.isEmpty() ? null : lookupColumns.get(0);
    }

    public List<ForeignKey> findForeignKeysByTableName(String tableName) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource()
                .addValue(TABLE_NAME, tableName, Types.VARCHAR);
        return namedParameterJdbcTemplate.query(FIND_FOREIGN_KEYS_BY_TABLE_NAME, namedParams, foreignKeyRowMapper);
    }

    private String sanitizeSqlIdentifier(String identifier) {
        String sanitized = identifier.toUpperCase(Locale.ROOT);
        if (!sanitized.matches("[A-Z][A-Z0-9_$#]*")) {
            throw new IllegalArgumentException("Unsafe SQL identifier: " + identifier);
        }
        return sanitized;
    }

}
