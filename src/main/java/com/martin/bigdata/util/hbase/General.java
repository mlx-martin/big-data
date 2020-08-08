package com.martin.bigdata.util.hbase;

import com.martin.bigdata.util.HbaseUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author martin
 */
public class General {

    private static final Logger logger = LoggerFactory.getLogger(HbaseUtil.class);

    /**
     * 创建命名空间
     *
     * @param nsName 命名空间名称
     * @throws Exception
     */
    public static void createNamespace(Connection connection, String nsName) throws Exception {
    }
}
