package com.martin.bigdata.util;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author martin
 * @desc hbase api
 */
public class HBaseUtil {

    private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);

    /**
     * 创建命名空间
     *
     * @param nsName 命名空间名称
     * @throws Exception
     */
    public static void createNamespace(Connection connection,String nsName) throws Exception {
        // 1 创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nsName).build();
        // 2 创建命名空间
        connection.getAdmin().createNamespace(namespaceDescriptor);
        logger.info("命名空间 " + nsName + " 创建成功！");
    }

    /**
     * 删除命名空间
     *
     * @param nsName 命名空间名称
     * @throws Exception
     */
    public static void dropNamespace(Connection connection,String nsName) throws Exception {
        connection.getAdmin().deleteNamespace(nsName);
        logger.info("命名空间 " + nsName + " 删除成功！");
    }

    /**
     * HBase 2.x 创建表
     *
     * @param tableName
     * @param cfs
     * @throws Exception
     */
    public static void createTable(Admin admin, String tableName, String... cfs)
            throws Exception {


        // 1 判断传入的列族信息是否为空
        if (cfs.length <= 0) {
            throw new RuntimeException("传入的列族信息是否为空");
        }
        // 2 判断表是否存在
        if (admin.tableExists(TableName.valueOf(tableName))) {
            throw new RuntimeException("无法创建已存在的表");
        }
        // 3 创建表描述器
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));

        // 4 循环添加列族信息
        for (String cf : cfs) {
            // 4.1 创建列族描述器
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build();
            // 4.2 添加列族信息
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }
        // 5 创建表
        admin.createTable(tableDescriptorBuilder.build());

        logger.info("表 " + tableName + " 创建成功！");

    }

    /**
     * HBase 2.x 创建包括 MOB 列的表
     *
     * @param tableName
     * @param cfs
     * @throws Exception
     */
    public static void createTableWithMOB(Admin admin,String tableName, String mobCf, String... cfs)
            throws Exception {

        // 1 判断传入的列族信息是否为空
        if (cfs.length <= 0) {
            throw new RuntimeException("传入的列族信息是否为空");
        }
        // 2 判断表是否存在
        if (admin.tableExists(TableName.valueOf(tableName))) {
            throw new RuntimeException("无法创建已存在的表");
        }
        // 3 创建表描述器
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));

        // 4 设置 mob 列
        HColumnDescriptor mobHColumnDescriptor = new HColumnDescriptor(mobCf);
        mobHColumnDescriptor.setMobEnabled(true);
        mobHColumnDescriptor.setMobThreshold(102400);
        tableDescriptorBuilder.setColumnFamily(mobHColumnDescriptor);

        // 5 循环添加列族信息
        for (String cf : cfs) {
            // 4.1 创建列族描述器
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build();
            // 4.2 添加列族信息
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        }
        // 6 创建表
        admin.createTable(tableDescriptorBuilder.build());

        logger.info("表 " + tableName + " 创建成功！");

    }


    /**
     * 删除表
     *
     * @param tableName 表名
     * @throws Exception
     */
    public static void dropTable(Admin admin,String tableName) throws IOException {

        // 1 判断表是否存在
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            throw new RuntimeException("无法删除不存在的表");
        }
        // 2 使表下线 disable
        admin.disableTable(TableName.valueOf(tableName));
        // 3 删除表
        admin.deleteTable(TableName.valueOf(tableName));

        System.out.println("表 " + tableName + " 删除成功！");

    }

    /**
     * 扫描表 2.x
     *
     * @param tableName 表名
     * @param startRow
     * @param stopRow
     * @throws IOException
     */
    public static void scanTable(Connection connection,String tableName, String startRow, String stopRow) throws IOException {
        // 1 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 2 创建 Scan 对象；可传入row区间或filter，默认扫描全表
        Scan scan = new Scan().withStartRow(Bytes.toBytes(startRow)).withStopRow(Bytes.toBytes(stopRow));
        // 3 扫描全表
        ResultScanner resultScanner = table.getScanner(scan);
        // 4 解析 resultScanner 获取 result
        for (Result result : resultScanner) {
            // 4 解析 result 并打印
            for (Cell cell : result.rawCells()) {
                // 5 打印数据
                System.out.println(
                        "ROW" + Bytes.toString(CellUtil.cloneRow(cell)) +
                                ", CF" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                                ", CN" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                                ", Value" + Bytes.toString(CellUtil.cloneValue(cell))
                );
            }
        }

        table.close();
    }

    /**
     * 插入普通数据
     *
     * @param tableName    表名
     * @param rowKey       行键
     * @param columnFamily 列簇
     * @param qualifier    列名
     * @param value        值
     * @throws IOException
     */
    public static void putData(Connection connection,String tableName, String rowKey, String columnFamily, String qualifier, String value) throws IOException {
        // 1 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 2 创建 Put 对象
        Put put = new Put(Bytes.toBytes(rowKey));
        // 3 给 Put 对象赋值
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        // 4 传入一个put对象，给但行数据多个列族添加数据；传入一个put对象的list，给多行数据多个列族添加数据
        table.put(put);
        // 5 释放资源
        table.close();
    }

    /**
     * 插入图片数据
     *
     * @param tableName    表名
     * @param rowKey       行键
     * @param columnFamily 列簇
     * @param qualifier    列名
     * @param value        值
     * @throws IOException
     */
    public static void putPictureData(Connection connection,String tableName, String rowKey, String columnFamily, String qualifier, String value) throws IOException {
        // 1 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 2 创建 Put 对象
        Put put = new Put(Bytes.toBytes(rowKey));
        // 3 给 Put 对象赋值
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        // 4 传入一个put对象，给但行数据多个列族添加数据；传入一个put对象的list，给多行数据多个列族添加数据
        table.put(put);
        // 5 释放资源
        table.close();
    }

    /**
     * 获取数据
     *
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     * @throws IOException
     */
    public static void getData(Connection connection,String tableName, String rowKey, String cf, String cn) throws IOException {
        // 1 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 2 创建 Get 对象
        Get get = new Get(Bytes.toBytes(rowKey));
//        // 2.1 指定列族（可选）
//        get.addFamily(Bytes.toBytes(cf));
//        // 2.2 指定列族和列（可选）
//        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
//        // 2.3 获取所有版本（可选）
//        get.setMaxVersions();
//        // 2.4 获取指定个数的版本（可选）
//        get.setMaxVersions(2);
        // 3 获取数据
        Result result = table.get(get);
        // 4 解析 result 并打印
        for (Cell cell : result.rawCells()) {
            // 5 打印数据
            System.out.println(
                    "ROW" + Bytes.toString(CellUtil.cloneRow(cell)) +
                            ", CF" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                            ", CN" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                            ", Value" + Bytes.toString(CellUtil.cloneValue(cell))
            );
        }

        table.close();
    }


    /**
     * 删除数据
     *
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     * @throws IOException
     */
    public static void deleteData(Connection connection,String tableName, String rowKey, String cf, String cn) throws IOException {
        // 1 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 2 创建 Delete 对象，默认删除指定行所有列所有版本
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 2.1 删除指定列族，默认删除指定列族的所有列的所有版本
        delete.addFamily(Bytes.toBytes(cf));
        // 2.2 删除指定列的单个版本，不指定时间戳默认删除最大版本
        delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        // 2,3 删除指定列的所有版本，指定时间戳，则删除时间戳小于等于的所有版本
        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));
        // 4 传入一个put对象，给但行数据多个列族添加数据；传入一个put对象的list，给多行数据多个列族添加数据
        table.delete(delete);
        table.close();
    }
}
