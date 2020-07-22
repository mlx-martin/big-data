package com.martin.bigdata.util;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author martin
 * @desc hdfs api
 */
public class HdfsUtil {

    /**
     * 遍历指定目录
     * @param fileSystem
     * @param path
     * @throws Exception
     */
    public static void listFiles(FileSystem fileSystem, String path) throws Exception {
        // 获取 RemoteIterator 得到所有的文件或者文件夹
        // 第一个参数指定遍历的路径
        // 第二个参数表示是否要递归遍历
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path(path), true);

        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = locatedFileStatusRemoteIterator.next();
            //获取每个文件的存储路径
            System.out.println(locatedFileStatus.getPath().toString());
            //获取文件的名字
            System.out.println(locatedFileStatus.getPath().getName());
            //获取文件的 block 存储信息
            BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
            System.out.println("该文件的 block 数量为：" + blockLocations.length);
            //获取每个 block 副本的存储位置
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
        }
    }

    /**
     * 创建目录
     * @param fileSystem
     * @param path
     * @throws Exception
     */
    public static void mkdirs(FileSystem fileSystem, String path) throws Exception {
        fileSystem.mkdirs(new Path(path));
    }

    /**
     * 将文件输出到流
     *
     * @param fileSystem
     * @param inputPath
     * @param outputStream
     * @throws Exception
     */
    public static void getFileOutputStream(FileSystem fileSystem, String inputPath, OutputStream outputStream) throws Exception {
        // 获取 hdfs 文件的输入流
        InputStream inputStream = fileSystem.open(new Path(inputPath));
        // 将输入流的数据复制到输出流，且自动关闭输入输出流
        IOUtils.copyBytes(inputStream, outputStream,4096,true);
    }


}
