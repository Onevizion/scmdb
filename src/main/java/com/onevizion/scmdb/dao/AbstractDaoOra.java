package com.onevizion.scmdb.dao;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Repository
public class AbstractDaoOra {
    @Resource
    protected JdbcTemplate jdbcTemplate;

    @Resource
    protected NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    private final static int LIMIT_IN_STATEMENT = 1000;

    protected <T> String appendIn(String columnName, List<T> list, Map<String, Object> outSqlParams) {
        int countParams = list.size() / LIMIT_IN_STATEMENT;
        int residue = list.size() % LIMIT_IN_STATEMENT;
        String paramName = "p_" + columnName;
        String prefix = "";
        StringBuilder sql = new StringBuilder(" (");
        for (int i = 0; i < countParams; i++) {
            sql.append(prefix);
            sql.append(" ");
            sql.append(columnName);
            sql.append(" in ");
            sql.append("(:");
            sql.append(paramName);
            sql.append(i);
            sql.append(") ");
            outSqlParams.put(paramName + i, list.subList(i * LIMIT_IN_STATEMENT, i * LIMIT_IN_STATEMENT + LIMIT_IN_STATEMENT));
            prefix = " or ";
        }
        if (countParams == 0) {
            if (list.isEmpty()) {
                list = null;
            }
            sql.append(" ");
            sql.append(columnName);
            sql.append(" in ");
            sql.append("(:");
            sql.append(paramName);
            sql.append(")) ");
            outSqlParams.put(paramName, list);
        } else if (residue != 0) {
            sql.append(" or ");
            sql.append(columnName);
            sql.append(" in ");
            sql.append("(:");
            sql.append(paramName);
            sql.append(countParams);
            sql.append(")) ");
            outSqlParams.put(paramName + countParams, list.subList(countParams * LIMIT_IN_STATEMENT, list.size()));
        } else {
            sql.append(") ");
        }
        return sql.toString();
    }
}
