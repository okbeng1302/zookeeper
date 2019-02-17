package com.beng.zookeeper.server;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * 服务端
 * 
 * @author apple
 */
public class DistributeServer {

    private String connectString = "127.0.0.1:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributeServer server = new DistributeServer();

        // 1. 连接 zookeeper 服务器
        server.connect();
        // 2. 注册节点
        server.register(args[0]);
        // 3. 业务逻辑
        server.process();
    }

    private void process() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * @param hostname
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void register(String hostname) throws KeeperException, InterruptedException {
        String path = zkClient.create("/servers/server1", hostname.getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + " is online ...");
    }

    /**
     * @throws IOException
     */
    private void connect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
    }
}
