package com.beng.zookeeper.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * 客户端
 * 
 * @author apple
 */
public class DistributeClient {

    private String connectString = "127.0.0.1:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    public static void main(String[] args) throws Exception {
        DistributeClient client = new DistributeClient();
        // 1. 获取 zookeeper 连接
        client.connect();
        // 2. 注册监听
        client.getChildren();
        // 3. 处理业务逻辑
        client.process();
    }

    private void process() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getChildren() throws KeeperException, InterruptedException {
        List<String> childrens = zkClient.getChildren("/servers", true);
        // 存储服务器节点集合
        List<String> hosts = new ArrayList<>();
        for (String child : childrens) {
            byte[] data = zkClient.getData("/servers/" + child, false, null);
            hosts.add(new String(data));
        }
        // 将所有在线主机名打印
        System.out.println(hosts);
    }

    /**
     * 连接 zookeeper 服务器
     * 
     * @throws IOException
     */
    private void connect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    getChildren();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
