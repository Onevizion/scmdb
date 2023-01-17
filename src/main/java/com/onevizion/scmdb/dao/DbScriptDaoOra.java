package com.onevizion.scmdb.dao;

import com.onevizion.scmdb.exception.DbConnectionException;
import com.onevizion.scmdb.vo.ScriptStatus;
import com.onevizion.scmdb.vo.ScriptType;
import com.onevizion.scmdb.vo.SqlScript;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections4.KeyValue;
import org.apache.commons.collections4.keyvalue.DefaultKeyValue;
import org.apache.commons.io.FileUtils;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Repository
public class DbScriptDaoOra extends AbstractDaoOra {

    private static final String UPDATE = "update db_script set file_hash = :fileHash,text = :text,ts = :ts where db_script_id = :id";
    private static final String CREATE = "insert into db_script (name,file_hash,text,ts,output,type,status) values (:name,:fileHash,:text,:ts,:output,:typeId,:statusId)";
    private static final String DELETE = "delete from db_script where db_script_id = ?";
    private static final String READ_ALL = "select db_script_id,name,ts,type,status from db_script";
    private static final String READ_TEXT_BY_ID = "select db_script_id,text from db_script where db_script_id in (:ids)";
    private static final String READ_SCRIPT_TEXT = "select text from db_script where db_script_id = ?";
    private static final String READ_COUNT = "select count(*) from db_script";

    private final RowMapper<SqlScript> rowMapperReadAll = (rs, rowNum) -> {
        SqlScript script = new SqlScript();
        script.setId(rs.getLong("db_script_id"));
        script.setName(rs.getString("name"));
        script.setTs(rs.getDate("ts"));
        script.setType(ScriptType.getById(rs.getLong("type")));
        script.setStatus(ScriptStatus.getById(rs.getLong("status")));
        return script;
    };
    RowMapper<KeyValue<Long, String>> rowMapperReadTextById = (rs, n) -> new DefaultKeyValue<>(rs.getLong("db_script_id"), rs.getString("text"));

    @Cacheable("dbScriptsMap")
    public Map<String, SqlScript> readMapCached() {
        return jdbcTemplate.query(READ_ALL, rs -> {
            Map<String, SqlScript> dbScripts = new HashMap<>();
            while (rs.next()) {
                SqlScript dbScript = rowMapperReadAll.mapRow(rs, rs.getRow());
                dbScripts.put(dbScript.getName(), dbScript);
            }
            return dbScripts;
        });
    }

    public Map<Long, String> readTextMapByIds(Set<Long> ids) {
        MapSqlParameterSource parameters = new MapSqlParameterSource();
        parameters.addValue("ids", ids);
        return namedParameterJdbcTemplate.query(READ_TEXT_BY_ID, parameters, rs -> {
            Map<Long, String> scripts = new HashMap<>();
            while (rs.next()) {
                KeyValue<Long, String> kv = rowMapperReadTextById.mapRow(rs, rs.getRow());
                scripts.put(kv.getKey(), kv.getValue());
            }
            return scripts;
        });
    }

    public Long readCount() {
        return jdbcTemplate.queryForObject(READ_COUNT, Long.class);
    }

    private MapSqlParameterSource mapScriptToMapParameter(SqlScript script, boolean isReadAllFilesContent) {
        MapSqlParameterSource source = new MapSqlParameterSource();
        source.addValue("name", script.getName());
        source.addValue("output", null);
        source.addValue("ts", script.getTs());
        source.addValue("typeId", script.getType().getId());
        source.addValue("statusId", script.getStatus().getId());
        String text = script.getTextFromFile();
        source.addValue("text", text);
        source.addValue("fileHash", SqlScript.getHashFromText(text));
        return source;
    }

    public void createAll(Collection<SqlScript> scripts, boolean isReadAllFilesContent) {
        namedParameterJdbcTemplate.batchUpdate(CREATE, scripts.stream()
                .map(script -> mapScriptToMapParameter(script, isReadAllFilesContent))
                .toArray(MapSqlParameterSource[]::new));
    }

    public void create(SqlScript script, boolean isReadAllFilesContent) {
        namedParameterJdbcTemplate.update(CREATE, mapScriptToMapParameter(script, isReadAllFilesContent));
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

    public void batchUpdate(List<SqlScript> scripts, boolean isReadAllFilesContent) {
        namedParameterJdbcTemplate.batchUpdate(UPDATE, scripts.stream()
                .map(script -> mapScriptToMapParameter(script, isReadAllFilesContent))
                .toArray(MapSqlParameterSource[]::new));
    }

    public void delete(Long id) {
        jdbcTemplate.update(DELETE, id);
    }

    public boolean isScriptTableExist() throws Exception {
        DataSource dataSource = jdbcTemplate.getDataSource();
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData dbMetaData = connection.getMetaData();
            ResultSet rs = dbMetaData.getTables(null, connection.getSchema(), "DB_SCRIPT",
                    new String[]{"TABLE"});
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