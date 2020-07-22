package com.martin.bigdata.config;

import org.apache.hadoop.fs.FileSystem;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

/**
 * @author martin
 * @desc hdfs 连接配置类
 */
@Configuration
@ConfigurationProperties(prefix = "hdfs.ha")
public class HdfsConfig {

    private  String user;
    private  String defaultFs;
    private  String nameservices;
    private  String namenodes;
    private  String rpcAddress1;
    private  String rpcAddress2;
    private  String clientFailoverProxyProvider;
    private  String clientUseDatanodeHostname;


    @Bean("haHdfs")
    public FileSystem getHaHdfs() throws Exception {

        //1 创建配置对象
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();

        //2 添加配置
        //2.1 配置 hdfs
        configuration.set("fs.defaultFS", defaultFs);
        configuration.set("dfs.nameservices", nameservices);
        configuration.set("dfs.ha.namenodes.hahdfs", namenodes);
        configuration.set("dfs.namenode.rpc-address.hahdfs.nn1", rpcAddress1);
        configuration.set("dfs.namenode.rpc-address.hahdfs.nn2", rpcAddress2);
        configuration.set("dfs.client.failover.proxy.provider.hahdfs",
                clientFailoverProxyProvider);
        //2.2 外域机器通信需要用外网 ip，未配置 hostname 访问会访问异常
        configuration.set("dfs.client.use.datanode.hostname", clientUseDatanodeHostname);

        //3 获取文件系统对象，默认为本地文件系统
        FileSystem fileSystem = FileSystem.get(new URI(defaultFs), configuration, user);

        return fileSystem;
    }


    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getDefaultFs() {
        return defaultFs;
    }

    public void setDefaultFs(String defaultFs) {
        this.defaultFs = defaultFs;
    }

    public String getNameservices() {
        return nameservices;
    }

    public void setNameservices(String nameservices) {
        this.nameservices = nameservices;
    }

    public String getNamenodes() {
        return namenodes;
    }

    public void setNamenodes(String namenodes) {
        this.namenodes = namenodes;
    }

    public String getRpcAddress1() {
        return rpcAddress1;
    }

    public void setRpcAddress1(String rpcAddress1) {
        this.rpcAddress1 = rpcAddress1;
    }

    public String getRpcAddress2() {
        return rpcAddress2;
    }

    public void setRpcAddress2(String rpcAddress2) {
        this.rpcAddress2 = rpcAddress2;
    }

    public String getClientFailoverProxyProvider() {
        return clientFailoverProxyProvider;
    }

    public void setClientFailoverProxyProvider(String clientFailoverProxyProvider) {
        this.clientFailoverProxyProvider = clientFailoverProxyProvider;
    }

    public String getClientUseDatanodeHostname() {
        return clientUseDatanodeHostname;
    }

    public void setClientUseDatanodeHostname(String clientUseDatanodeHostname) {
        this.clientUseDatanodeHostname = clientUseDatanodeHostname;
    }
}
