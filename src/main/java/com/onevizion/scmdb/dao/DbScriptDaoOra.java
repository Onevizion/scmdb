package com.onevizion.scmdb.dao;

import com.onevizion.scmdb.exception.DbConnectionException;
import com.onevizion.scmdb.vo.ScriptStatus;
import com.onevizion.scmdb.vo.ScriptType;
import com.onevizion.scmdb.vo.SqlScript;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

@Repository
public class DbScriptDaoOra extends AbstractDaoOra {

    private static final String UPDATE = "update db_script set file_hash = :fileHash,text = :text,ts = :ts where db_script_id = :id";
    private static final String CREATE = "insert into db_script (name,file_hash,text,ts,output,type,status) values (:name,:fileHash,:text,:ts,:output,:type.id,:status.id)";
    private static final String DELETE = "delete from db_script where db_script_id = ?";
    private static final String READ_ALL = "select db_script_id,name,file_hash,null as text,ts,type,status from db_script";
    private static final String READ_ALL_BY_ID = "select db_script_id,name,file_hash,text,ts,type,status from db_script where db_script_id in (:ids)";
    private static final String READ_SCRIPT_TEXT = "select text from db_script where db_script_id = ?";
    private static final String READ_COUNT = "select count(*) from db_script";

    private final RowMapper<SqlScript> rowMapper = (rs, rowNum) -> {
        SqlScript script = new SqlScript();
        script.setId(rs.getLong("db_script_id"));
        script.setName(rs.getString("name"));
        script.setFileHash(rs.getString("file_hash"));
        script.setText(rs.getString("text"));
        script.setTs(rs.getDate("ts"));
        script.setType(ScriptType.getById(rs.getLong("type")));
        script.setStatus(ScriptStatus.getById(rs.getLong("status")));
        return script;
    };

    private ResultSetExtractor<Map<String, SqlScript>> dbScriptsExtractor = rs -> {
        Map<String, SqlScript> dbScripts = new HashMap<>();
        while (rs.next()) {
            SqlScript dbScript = rowMapper.mapRow(rs, rs.getRow());
            dbScripts.put(dbScript.getName(), dbScript);
        }
        return dbScripts;
    };

    @Cacheable("dbScriptsMap")
    public Map<String, SqlScript> readMapCached() {
        return jdbcTemplate.query(READ_ALL, dbScriptsExtractor);
    }

    public Map<String, SqlScript> readMapByIds(Set<Long> ids) {
        MapSqlParameterSource parameters = new MapSqlParameterSource();
        parameters.addValue("ids", ids);

        return namedParameterJdbcTemplate.query(READ_ALL_BY_ID, parameters, dbScriptsExtractor);
    }

    public Long readCount() {
        return jdbcTemplate.queryForObject(READ_COUNT, Long.class);
    }

    public void createAll(Collection<SqlScript> scripts) {
        SqlScript[] dbScriptsArr = scripts.toArray(new SqlScript[scripts.size()]);
        namedParameterJdbcTemplate.batchUpdate(CREATE, SqlParameterSourceUtils.createBatch(dbScriptsArr));
    }

    public void create(SqlScript script) {
        namedParameterJdbcTemplate.update(CREATE, new BeanPropertySqlParameterSource(script));
    }

    public void deleteByIds(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return;
        }
        Map<String, Object> params = new HashMap<>();
        String sql = "delete from db_script where ";
        sql += appendIn("db_script_id", ids, params);
        namedParameterJdbcTemplate.update(sql, params);
    }

    public void batchUpdate(List<SqlScript> scripts) {
        SqlScript[] scriptsArr = scripts.toArray(new SqlScript[scripts.size()]);
        namedParameterJdbcTemplate.batchUpdate(UPDATE, SqlParameterSourceUtils.createBatch(scriptsArr));
    }

    public void delete(Long id) {
        jdbcTemplate.update(DELETE, id);
    }

    public boolean isScriptTableExist() throws Exception {
        DataSource dataSource = jdbcTemplate.getDataSource();
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData dbMetaData = connection.getMetaData();
            ResultSet rs = dbMetaData.getTables(null, connection.getSchema(), "DB_SCRIPT",
                new String[] {"TABLE"});
            return rs.next();
        } catch (SQLException e) {
            throw new Exception("Can't establish a connection to the database by the parameters given");
        }
    }

    public void checkDbConnection() {
        try (Connection ignored = jdbcTemplate.getDataSource().getConnection()) {
        } catch (SQLException e) {
            throw new DbConnectionException("Cannot establish DB connection. " + e.getMessage(), e);
        }
    }
}
