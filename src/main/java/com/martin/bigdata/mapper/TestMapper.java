package com.martin.bigdata.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

@Repository
@Mapper
public interface TestMapper {


    void test(
            @Param("uuid") String uuid
    );

    void createTableTest(
            @Param("tableName") String tableName
    );

}