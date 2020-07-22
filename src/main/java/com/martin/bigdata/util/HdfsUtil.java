package com.martin.bigdata.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

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


    /**
     * 第一种方法
     *
     * @param fileSystem
     * @param inputPath
     * @param outputPath
     * @throws Exception
     */
    public static void getFile1(FileSystem fileSystem, String inputPath, String outputPath) throws Exception {
        // 获取 hdfs 文件的输入流
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(inputPath));
        // 获取本地文件的输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File(outputPath));
        // 将输入流的数据复制到输出流
        org.apache.commons.io.IOUtils.copy(fsDataInputStream, fileOutputStream);
        // 关闭流
        org.apache.commons.io.IOUtils.closeQuietly(fsDataInputStream);
        org.apache.commons.io.IOUtils.closeQuietly(fileOutputStream);
    }

    /**
     * 第二种方法
     *
     * @param fileSystem
     * @param inputPath
     * @param outputPath
     * @throws Exception
     */
    public static void getFile2(FileSystem fileSystem, String inputPath, String outputPath) throws Exception {
        // 获取 hdfs 文件的输入流
        fileSystem.copyToLocalFile(false, new Path(inputPath), new Path(outputPath), true);

    }


    public static void putFile(FileSystem fileSystem, String inputPath, String outputPath) throws Exception {
        // 获取 hdfs 文件的输入流
        fileSystem.copyFromLocalFile(new Path(inputPath), new Path(outputPath));

    }

    public void mergeFile() throws Exception {
        //1 获取分布式文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://101.200.169.243:9000"), new Configuration(), "root");
        //2 创建大文件输出流
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/bigfile.xml"));
        //3 获取本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        //4 通过本地文件系统获取文件列表，为一个集合，将文件输入流复制到输出流
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("/User/martin/input"));
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = localFileSystem.open(fileStatus.getPath());
            org.apache.commons.io.IOUtils.copy(inputStream, fsDataOutputStream);
            org.apache.commons.io.IOUtils.closeQuietly(inputStream);
        }
        org.apache.commons.io.IOUtils.closeQuietly(fsDataOutputStream);
        localFileSystem.close();
        fileSystem.close();
    }

}
