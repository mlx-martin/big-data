package com.martin.bigdata;

import com.martin.bigdata.config.PhoenixConfig;
import com.martin.bigdata.util.HbaseUtil;
import com.martin.bigdata.util.HdfsUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import javax.sql.DataSource;

@SpringBootTest
@RunWith(SpringRunner.class)
public class BigDataApplicationTests {

    @Resource(name = "haHdfs")
    FileSystem haHdfs;

    @Resource(name = "hbase")
    Connection hbase;

    @Autowired
    TestService testService;

    @Autowired
    DataSource phoenixDataSource;

    @Autowired
    PhoenixConfig phoenixConfig;

    @Test
    public void listFilesTest() throws Exception {
        HdfsUtil.listFiles(haHdfs, "/");
    }

    @Test
    public void createNamespaceTest() throws Exception {
        System.out.println(hbase.getConfiguration());
        HbaseUtil.createNamespace(hbase, "ns3");
    }

    @Test
    public void testMapperTest() {
//        testService.createTableTest();
        testService.test();
    }



}
