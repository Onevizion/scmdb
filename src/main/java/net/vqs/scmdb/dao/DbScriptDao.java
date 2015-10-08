package net.vqs.scmdb.dao;

import net.vqs.scmdb.vo.DbScriptVo;
import net.vqs.scmdb.vo.DbScriptStatus;
import net.vqs.scmdb.vo.DbScriptType;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Resource;

@Component
public class DbScriptDao {
    private final String DELETE_BY_IDS = "delete from db_script where db_script_id in (:p_ids)";
    private final String CREATE = "insert into db_script (name, file_hash, text, ts, output, type, status) values (?,?,?,?,?,?,?)";
    private final String READ_ALL = "select * from db_script";
    private final String READ_NEWEST = "select * from (select * from db_script order by name desc, ts desc) where rownum = 1";

    @Resource
    JdbcTemplate jdbcTemplate;

    @Resource
    NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private RowMapper<DbScriptVo> rowMapper = new RowMapper<DbScriptVo>() {
        public DbScriptVo mapRow(ResultSet rs, int i) throws SQLException {
            DbScriptVo dbScript = new DbScriptVo();
            dbScript.setDbScriptId(rs.getLong("DB_SCRIPT_ID"));
            dbScript.setName(rs.getString("NAME"));
            dbScript.setFileHash(rs.getString("FILE_HASH"));
            dbScript.setText(rs.getString("TEXT"));
            dbScript.setTs(rs.getDate("TS"));
            dbScript.setOutput(rs.getString("OUTPUT"));
            dbScript.setType(DbScriptType.getForInt(rs.getInt("TYPE")));
            dbScript.setStatus(DbScriptStatus.getForInt(rs.getInt("STATUS")));
            return dbScript;
        }
    };

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

    public DbScriptVo readNewest() {
        return jdbcTemplate.queryForObject(READ_NEWEST, rowMapper);
    }

    public void batchCreate(final DbScriptVo[] dbScripts) {
        jdbcTemplate.batchUpdate(CREATE, new BatchPreparedStatementSetter() {
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setString(1, dbScripts[i].getName());
                ps.setString(2, dbScripts[i].getFileHash());
                ps.setString(3, dbScripts[i].getText());
                ps.setDate(4, new Date(dbScripts[i].getTs().getTime()));
                ps.setString(5, dbScripts[i].getOutput());
                ps.setInt(6, dbScripts[i].getType().getTypeId());
                ps.setInt(7, dbScripts[i].getStatus().getStatusId());
            }

            public int getBatchSize() {
                return dbScripts.length;
            }
        });

    }

    public void deleteByIds(Collection<Long> delIds) {
        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("p_ids", delIds);
        namedParameterJdbcTemplate.update(DELETE_BY_IDS, params);
    }
}
