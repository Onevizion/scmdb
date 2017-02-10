package com.onevizion.scmdb.dao;

import com.onevizion.scmdb.vo.SqlScript;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Repository
public class SqlScriptDaoOra extends AbstractDaoOra {

    private String UPDATE = "update db_script set name = :name,file_hash = :fileHash,text = :text,ts = :ts,output = :output,type = :type,status = :status.id where db_script_id = :id";

    private String CREATE = "insert into db_script (name,file_hash,text,ts,output,type,status) values (:name,:fileHash,:text,:ts,:output,:type,:status)";

    private final String DELETE_BY_IDS = "delete from db_script where db_script_id in (:p_ids)";
    private final String READ_ALL = "select * from db_script";
    private final String READ_COUNT = "select count(*) from db_script";
    private final String READ_NEWEST = "select * from (select * from db_script order by name desc, ts desc) where rownum = 1";

    private RowMapper<SqlScript> rowMapper = new BeanPropertyRowMapper<>(SqlScript.class);

    private ResultSetExtractor<Map<String, SqlScript>> dbScriptsExtractor = rs -> {
        Map<String, SqlScript> dbScripts = new HashMap<>();
        while (rs.next()) {
            SqlScript dbScript = rowMapper.mapRow(rs, rs.getRow());
            dbScripts.put(dbScript.getName(), dbScript);
        }
        return dbScripts;
    };

    public Map<String, SqlScript> readMap() {
        return jdbcTemplate.query(READ_ALL, dbScriptsExtractor);
    }

    public Long readCount() {
        return jdbcTemplate.queryForObject(READ_COUNT, Long.class);
    }

    public void batchCreate(Collection<SqlScript> dbScripts) {
        SqlScript[] dbScriptsArr = dbScripts.toArray(new SqlScript[dbScripts.size()]);
        //jdbcTemplate.batchUpdate(CREATE, new Ba);
    }

    public void create(SqlScript dbScript) {
        //jdbcTemplate.update(CREATE, );
    }

    public void deleteByIds(Collection<Long> delIds) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("p_ids", delIds);
        namedParameterJdbcTemplate.update(DELETE_BY_IDS, params);
    }

    public void update(SqlScript dbScript) {
        //jdbcTemplate.update(UPDATE,);
    }
}
