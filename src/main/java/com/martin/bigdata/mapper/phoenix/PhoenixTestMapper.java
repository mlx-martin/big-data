package com.martin.bigdata.mapper.phoenix;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

/**
 * @author martin
 * @desc phoenix 测试 mapper
 */
@Repository
@Mapper
public interface PhoenixTestMapper {


    void test(
            @Param("uuid") String uuid
    );

    void createTableTest(
            @Param("tableName") String tableName
    );

}