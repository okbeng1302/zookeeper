package com.beng.app;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

public class TestZookeeper {

    private String connectString = "127.0.0.1:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    // @Test
    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent arg0) {
                System.out.println("=======================");
                List<String> nodes;
                try {
                    nodes = zkClient.getChildren("/", false);
                    for (String node : nodes) {
                        System.out.println(node);
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        });
    }

    /**
     * 创建节点
     * 
     * @annotation 1. PERSISTENT 持久化节点 2. EPHEMERAL 临时节点 客户端断开就删除
     * @throws InterruptedException
     * @throws KeeperException
     */
    @Test
    public void create() throws KeeperException, InterruptedException {
        String path = zkClient.create("/beng", "xiaobai".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }

    /**
     * 监听节点
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    // @Test
    public void getDataAndWatch() throws KeeperException, InterruptedException {
        List<String> nodes = zkClient.getChildren("/", true);
        for (String node : nodes) {
            System.out.println(node);
        }
        Thread.sleep(100000L);
    }

    /**
     * 判断节点是否存在
     * 
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void exists() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/beng", false);
        System.out.println(stat == null ? "not exists" : "exists");
    }
}
