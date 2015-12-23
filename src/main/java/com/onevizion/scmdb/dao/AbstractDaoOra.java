package com.onevizion.scmdb.dao;

import com.onevizion.scmdb.TextFilePopulator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Repository
public abstract class AbstractDaoOra {
    @Resource
    protected JdbcTemplate jdbcTemplate;

    @Resource
    protected NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    @PostConstruct
    public void populateSqlStrings() {
        TextFilePopulator.populate(this.getClass());
    }
}
