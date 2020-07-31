package com.martin.bigdata.service;

import com.martin.bigdata.mapper.mysql.MysqlTestMapper;
import com.martin.bigdata.mapper.phoenix.PhoenixTestMapper;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.UUID;

/**
 * @author martin
 */
@Service
public class TestService {

    @Resource
    private PhoenixTestMapper phoenixTestMapper;

    @Resource
    private MysqlTestMapper mysqlTestMapper;

    public void phoenixTest() {
        phoenixTestMapper.test(UUID.randomUUID().toString().replaceAll("-", ""));
    }

    public void createTableTest() {
        phoenixTestMapper.createTableTest("mlxtest");
    }

    public void mysqlTest() {
        System.out.println(mysqlTestMapper.test());
    }
}
