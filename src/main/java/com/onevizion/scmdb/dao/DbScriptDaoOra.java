package com.onevizion.scmdb.dao;

import com.onevizion.scmdb.vo.DbScriptVo;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Repository
public class DbScriptDaoOra extends AbstractDaoOra {

    private String UPDATE = "update db_script set name = :name,file_hash = :fileHash,text = :text,ts = :ts,output = :output,type = :type,status = :status where db_script_id = :dbScriptId";

    private String CREATE = "insert into db_script (name,file_hash,text,ts,output,type,status) values (:name,:fileHash,:text,:ts,:output,:type,:status)";

    private final String DELETE_BY_IDS = "delete from db_script where db_script_id in (:p_ids)";
    private final String READ_ALL = "select * from db_script";
    private final String READ_COUNT = "select count(*) from db_script";
    private final String READ_NEWEST = "select * from (select * from db_script order by name desc, ts desc) where rownum = 1";

    private RowMapper<DbScriptVo> rowMapper = new BeanPropertyRowMapper<DbScriptVo>(DbScriptVo.class);

    private ResultSetExtractor<Map<String, DbScriptVo>> dbScriptsExtractor = new ResultSetExtractor<Map<String, DbScriptVo>>() {
        public Map<String, DbScriptVo> extractData(ResultSet rs) throws SQLException, DataAccessException {
            Map<String, DbScriptVo> dbScripts = new HashMap<String, DbScriptVo>();
            while (rs.next()) {
                DbScriptVo dbScript = rowMapper.mapRow(rs, rs.getRow());
                dbScripts.put(dbScript.getName(), dbScript);
            }
            return dbScripts;
        }
    };

    public Map<String, DbScriptVo> readAll() {
        return jdbcTemplate.query(READ_ALL, dbScriptsExtractor);
    }

    public Long readCount() {
        return jdbcTemplate.queryForObject(READ_COUNT, Long.class);
    }

    public void batchCreate(Collection<DbScriptVo> dbScripts) {
        DbScriptVo[] dbScriptsArr = dbScripts.toArray(new DbScriptVo[dbScripts.size()]);
        namedParameterJdbcTemplate.batchUpdate(CREATE, SqlParameterSourceUtils.createBatch(dbScriptsArr));
    }

    public void create(DbScriptVo dbScript) {
        namedParameterJdbcTemplate.update(CREATE, new BeanPropertySqlParameterSource(dbScript));
    }

    public void deleteByIds(Collection<Long> delIds) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("p_ids", delIds);
        namedParameterJdbcTemplate.update(DELETE_BY_IDS, params);
    }

    public void update(DbScriptVo dbScriptVo) {
        namedParameterJdbcTemplate.update(UPDATE, new BeanPropertySqlParameterSource(dbScriptVo));
    }
}
