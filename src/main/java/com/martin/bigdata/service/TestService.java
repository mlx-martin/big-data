package com.martin.bigdata.service;

import com.martin.bigdata.mapper.TestMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * @author martin
 */
@Service
public class TestService {

    @Autowired
    private TestMapper testMapper;

    public void test(){
        testMapper.test(UUID.randomUUID().toString().replaceAll("-", ""));
    }
    public void createTableTest(){
        testMapper.createTableTest("mlxtest");
    }

}
