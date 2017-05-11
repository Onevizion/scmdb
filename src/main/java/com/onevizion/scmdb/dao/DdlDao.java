package com.onevizion.scmdb.dao;

import com.onevizion.scmdb.vo.DbObject;
import com.onevizion.scmdb.vo.DbObjectType;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Component
public class DdlDao extends AbstractDaoOra {
    public void executeTransformParamStatements() {
        String plsqlBlock = "begin" +
                "\n dbms_metadata.set_transform_param(dbms_metadata.session_transform,'PRETTY',true);" +
                "\n dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SQLTERMINATOR',true);" +
                "\n dbms_metadata.set_transform_param(dbms_metadata.session_transform,'SEGMENT_ATTRIBUTES',false);" +
                "\n end;";
        jdbcTemplate.execute(plsqlBlock);
    }

    public List<DbObject> extractPackageBodiesDdls() {
        List<DbObject> dbObjects = jdbcTemplate.query("select object_name," +
                " dbms_metadata.get_ddl('PACKAGE_BODY', object_name)" +
                " from user_objects" +
                " where object_type = 'PACKAGE BODY'", new DbObjectExtractor());
        return dbObjects;
    }

    public List<DbObject> extractPackageSpecDdls() {
        List<DbObject> dbObjects = jdbcTemplate.query("select object_name," +
                " dbms_metadata.get_ddl('PACKAGE_SPEC', object_name)" +
                " from user_objects" +
                " where object_type = 'PACKAGE'", new DbObjectExtractor());
        return dbObjects;
    }

    public List<DbObject> extractSequencesDdls() {
        List<DbObject> dbObjects = jdbcTemplate.query("select trgrs.table_name," +
                " dbms_metadata.get_ddl('SEQUENCE', depends.referenced_name)" +
                " from user_dependencies depends, user_triggers trgrs" +
                " where trgrs.trigger_name = depends.name and depends.type = 'TRIGGER'" +
                " and depends.referenced_type = 'SEQUENCE' order by depends.referenced_name", new DbObjectExtractor());
        return dbObjects;
    }

    public List<DbObject> extractViewsDdls() {
        List<DbObject> dbObjects = jdbcTemplate.query("select view_name," +
                " dbms_metadata.get_ddl('VIEW', view_name)" +
                " from user_views", new DbObjectExtractor());
        return dbObjects;
    }

    public List<DbObject> extractTablesDdls() {
        List<DbObject> dbObjects = jdbcTemplate.query("select object_name," +
                        " dbms_metadata.get_ddl('TABLE', object_name)" +
                        " from user_objects where object_type='TABLE' and generated = 'N' and object_name not like 'Z_%'",
                new DbObjectExtractor()
        );
        return dbObjects;
    }

    public List<DbObject> extractTriggersDdls() {
        List<DbObject> dbObjects = jdbcTemplate.query("select table_name," +
                " dbms_metadata.get_ddl('TRIGGER', trigger_name)" +
                " from user_triggers" +
                " where table_name not like 'Z_%' and trigger_name not like 'Z_%'" +
                " order by trigger_name", new DbObjectExtractor());
        return dbObjects;
    }

    public List<DbObject> extractIndexesDdls() {
        List<DbObject> dbObjects = jdbcTemplate.query("select table_name, dbms_metadata.get_ddl('INDEX', index_name)" +
                " from user_indexes" +
                " where generated = 'N' and table_name not like 'Z_%' and index_name not like 'PK_%'" +
                " order by table_name asc, uniqueness desc, index_name asc", new DbObjectExtractor());
        return dbObjects;
    }

    public String extractDdlByNameAndType(String dbObjName, String dbObjType) {
        String sql = "select dbms_metadata.get_ddl(upper(:dbObjType), upper(:dbObjName)) from dual";
        MapSqlParameterSource namedParams = new MapSqlParameterSource();
        namedParams.addValue("dbObjName", dbObjName);
        namedParams.addValue("dbObjType", dbObjType);
        String ddl = namedParameterJdbcTemplate.queryForObject(sql, namedParams, String.class);
        return ddl;
    }

    public List<DbObject> extractTableDependentObjectsDdl(String tableName, String depObjType) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource();
        namedParams.addValue("tableName", tableName);
        namedParams.addValue("depObjType", depObjType);

        List<DbObject> dbObjects = null;
        if (DbObjectType.COMMENT.toString().equals(depObjType)) {
            String sql = "select table_name, dbms_metadata.get_dependent_ddl('COMMENT', table_name) from" +
                    " ((select table_name from user_tab_comments" +
                    "     where comments is not null)" +
                    " union" +
                    "  (select table_name from user_col_comments" +
                    "     where comments is not null" +
                    "     group by table_name)) where table_name = upper(:tableName)";
            dbObjects = namedParameterJdbcTemplate.query(sql, namedParams, new DbObjectExtractor());
        } else if (DbObjectType.SEQUENCE.toString().equals(depObjType)) {
            String sql = "select trgrs.table_name, dbms_metadata.get_ddl('SEQUENCE', depends.referenced_name)" +
                    " from user_dependencies depends, user_triggers trgrs" +
                    " where trgrs.trigger_name = depends.name and depends.type = 'TRIGGER'" +
                    " and depends.referenced_type = 'SEQUENCE' and trgrs.table_name = upper(:tableName)" +
                    " order by depends.referenced_name";
            dbObjects = namedParameterJdbcTemplate.query(sql, namedParams, new DbObjectExtractor());
        } else if (DbObjectType.INDEX.toString().equals(depObjType)) {
            String sql = "select table_name, dbms_metadata.get_ddl(upper(:depObjType), index_name)" +
                    " from user_indexes where generated = 'N' and table_name=upper(:tableName) and index_name not like 'PK_%'" +
                    " order by table_name asc, uniqueness desc, index_name asc";
            dbObjects = namedParameterJdbcTemplate.query(sql, namedParams, new DbObjectExtractor());
        } else if (DbObjectType.TRIGGER.toString().equals(depObjType)) {
            String sql = "select table_name, dbms_metadata.get_ddl(upper(:depObjType), trigger_name)" +
                    " from user_triggers where table_name=upper(:tableName)" +
                    " and trigger_name not like 'Z_%' order by trigger_name";
            dbObjects = namedParameterJdbcTemplate.query(sql, namedParams, new DbObjectExtractor());
        }

        return dbObjects;
    }

    public String getTableNameByDepObject(String objName, String objType) {
        MapSqlParameterSource namedParams = new MapSqlParameterSource();
        namedParams.addValue("objName", objName);
        String sql;
        if (DbObjectType.INDEX.toString().equals(objType)) {
            sql = "select table_name from user_indexes where index_name = upper(:objName)";
        } else if (DbObjectType.TRIGGER.toString().equals(objType)) {
            sql = "select table_name from user_triggers where trigger_name = upper(:objName)";
        } else if (DbObjectType.SEQUENCE.toString().equals(objType)) {
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

    public String getObjectTypeByName(String dbObjName) {
        String sql = "select 'TABLE' object_type from user_tables where table_name = upper(:dbObjName)" +
            " union all select 'VIEW' object_type from user_views where view_name = upper(:dbObjName)";
        MapSqlParameterSource namedParams = new MapSqlParameterSource();
        namedParams.addValue("dbObjName", dbObjName);
        String dbObjType = namedParameterJdbcTemplate.queryForObject(sql, namedParams, String.class);
        return dbObjType;
    }

    public List<DbObject> extractTabCommentsDdls() {
        List<DbObject> dbObjects = jdbcTemplate.query(
                "select table_name, dbms_metadata.get_dependent_ddl('COMMENT', table_name) from" +
                        "(select tab1.table_name, tab1.table_type from user_tab_comments tab1" +
                        "  inner join ((select table_name from user_tab_comments" +
                        "                 where comments is not null)" +
                        "               union" +
                        "              (select table_name from user_col_comments" +
                        "                  where comments is not null" +
                        "                  group by table_name)) tab2" +
                        "     on tab1.table_name=tab2.table_name)" +
                        "  where table_type='TABLE'" +
                        "  order by table_name", new DbObjectExtractor()
        );
        return dbObjects;
    }

    public List<DbObject> extractViewCommentsDdls() {
        List<DbObject> dbObjects = jdbcTemplate.query(
                "select table_name, dbms_metadata.get_dependent_ddl('COMMENT', table_name) from" +
                        "(select tab1.table_name, tab1.table_type from user_tab_comments tab1" +
                        "  inner join ((select table_name from user_tab_comments" +
                        "                 where comments is not null)" +
                        "               union" +
                        "              (select table_name from user_col_comments" +
                        "                  where comments is not null" +
                        "                  group by table_name)) tab2" +
                        "     on tab1.table_name=tab2.table_name)" +
                        "  where table_type='VIEW'" +
                        "  order by table_name",
                new DbObjectExtractor()
        );
        return dbObjects;
    }

    public boolean isExist(String objName, String objType) {
        if (DbObjectType.PACKAGE_SPEC.toString().equals(objType)) {
            objType = "PACKAGE";
        } else if (DbObjectType.PACKAGE_BODY.toString().equals(objType)) {
            objType = "PACKAGE BODY";
        }
        MapSqlParameterSource namedParams = new MapSqlParameterSource();
        namedParams.addValue("objName", objName);
        namedParams.addValue("objType", objType);
        String sql = "select case when" +
                "                count(object_name) > 0 then 'true'" +
                "            else 'false'" +
                "        end" +
                "    from user_objects where object_name = upper(:objName) and object_type = upper(:objType)";
        String boolStr = namedParameterJdbcTemplate.queryForObject(sql, namedParams, String.class);
        return Boolean.valueOf(boolStr);
    }

    private static final class DbObjectExtractor implements RowMapper<DbObject> {

        @Override
        public DbObject mapRow(ResultSet rs, int rowNum) throws SQLException {
            DbObject dbObject = new DbObject();
            dbObject.setName(rs.getString(1));
            dbObject.setDdl(rs.getString(2));
            return dbObject;
        }
    }
}