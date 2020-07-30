package com.martin.bigdata.config;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

@Configuration
@ConfigurationProperties(prefix = "zookeeper")
public class ZookeeperConfig {

    private String quorum;
    private int timeout;


    @Bean(name = "zookeeper")
    public ZooKeeper zooKeeper() throws IOException, InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        //  连接成功后，会回调watcher监听，此连接操作是异步的，执行完new语句后，直接调用后续代码
        ZooKeeper zooKeeper = new ZooKeeper(quorum, timeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (Event.KeeperState.SyncConnected == event.getState()) {
                    //如果收到了服务端的响应事件,连接成功
                    countDownLatch.countDown();
                }
            }
        });
        countDownLatch.await();
        return zooKeeper;
    }

    public String getQuorum() {
        return quorum;
    }

    public void setQuorum(String quorum) {
        this.quorum = quorum;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
}
