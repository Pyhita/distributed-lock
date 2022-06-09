package com.yangtao.zookeeper;

import com.yangtao.common.Lock;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;

import java.util.concurrent.CountDownLatch;
import java.util.spi.LocaleNameProvider;

/**
 * @Author: pyhita
 * @Date: 2022/6/7
 * @Descrption: com.yangtao.zookeeper
 * @Version: 1.0
 */

// 基于zk临时节点实现的分布式锁，存在羊群效应的问题！
public class LockZookeeper_Ephemeral implements Lock {

    private CountDownLatch latch = new CountDownLatch(1);
    private static volatile ZkClient zkClient;
    private static final String ROOT = "/locks";

    // 单例模式，保证zkClient是单例
    private LockZookeeper_Ephemeral() {}

    public static LockZookeeper_Ephemeral getInstance() {
        if (zkClient == null) {
            synchronized (LockZookeeper_Ephemeral.class) {
                if (zkClient == null) {
                    zkClient = new ZkClient("120.78.82.36:2181", 30000);
                    boolean flag = zkClient.exists(ROOT);
                    if (flag) {
                        zkClient.createPersistent(ROOT);
                    }
                }
            }
        }

        return new LockZookeeper_Ephemeral();
    }

    @Override
    public void getLock(String lockname) {
        try {
            // 抢锁:即创建临时节点
            zkClient.createEphemeral(ROOT + "/" + lockname);
        } catch (Exception e) {
            // 抢锁失败了：阻塞状态
            waitLock(lockname);
            // 重写获取锁的资源
            getLock(lockname);
        }
    }

    // 释放锁
    @Override
    public void unlock(String lockname) {
        zkClient.delete(ROOT + "/" + lockname);
    }

    // 阻塞
    private void waitLock(String lockname) {
        // 创建监听事件
        IZkDataListener zkDataListener = new IZkDataListener() {
            @Override
            public void handleDataChange(String s, Object o) throws Exception {

            }

            //节点删除事件
            public void handleDataDeleted(String path) throws Exception {
                latch.countDown();//唤醒
            }
        };

        zkClient.subscribeDataChanges(ROOT + "/" + lockname, zkDataListener);
        // 如果节点存在，则创建信号量，并且阻塞
        if (zkClient.exists(ROOT + "/" + lockname)) {
            latch = new CountDownLatch(1);
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // 唤醒之后，会执行这里，取消订阅监听事件
        zkClient.unsubscribeDataChanges(ROOT + "/" + lockname, zkDataListener);
    }
}
