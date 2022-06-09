package com.yangtao.zookeeper;

import com.yangtao.common.Lock;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @Author: pyhita
 * @Date: 2022/6/7
 * @Descrption: com.yangtao.zookeeper
 * @Version: 1.0
 */
// 基于zk临时有序节点实现的分布式锁
public class LockZookeeper_Ephemeral_Sequential implements Lock, Watcher {

    private ZooKeeper zooKeeper;
    // 根节点
    private String ROOT_LOCK = "/locks";
    // 竞争的资源
    private String lockName;
    // 等待的前一个锁
    private String WAIT_LOCK;
    // 当前锁
    private String CURRENT_LOCK;
    // 计数器
    private CountDownLatch countDownLatch;
    private int sessionTimeout = 30000;
    private List<Exception> exceptionList  = new ArrayList<>();

    /**
     *
     * @param config 连接的url
     * @param lockName 竞争的资源
     */
    public LockZookeeper_Ephemeral_Sequential(String config, String lockName) {
        this.lockName = lockName;
        try {
            // 连接zookeeper
            zooKeeper = new ZooKeeper(config, sessionTimeout, this);
            Stat stat = zooKeeper.exists(ROOT_LOCK, false);
            if (stat == null) {
                // 如果根节点不存在，先创建根节点
                zooKeeper.create(ROOT_LOCK, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void getLock(String lockname) throws InterruptedException {
        if (exceptionList.size() > 0) {
            throw new LockException(exceptionList.get(0));
        }

        try {
            if (this.tryLock()) {
                System.out.println(Thread.currentThread().getName() + lockname + " 获得了锁");
                return;
            } else {
                // 等待锁
                waitForLock(WAIT_LOCK, sessionTimeout);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public boolean tryLock() {
        try {
            String spilter = "_lock_";
            if (lockName.contains(spilter)) {
                throw new LockException("锁名有误！");
            }

            // 创建临时有序节点
            CURRENT_LOCK = zooKeeper.create(ROOT_LOCK + "/" + lockName + spilter, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(CURRENT_LOCK + "已创建完毕了！");
            // 取出所有的子节点
            List<String> children = zooKeeper.getChildren(ROOT_LOCK, false);
            // 子节点类似：lockname_lock_0000000006，lockname_lock_0000000007
            // 取出所有lockName的锁
            List<String> lockObjects = new ArrayList<>();
            for (String child : children) {
                String _node = child.split(spilter)[0];
                if (_node.equals(lockName)) {
                    lockObjects.add(child);
                }
            }
            Collections.sort(lockObjects);
            System.out.println(Thread.currentThread().getName() + " 的锁是 " + CURRENT_LOCK);
            // 如果当前节点是最小的节点，那么获取锁就成功了
            if (CURRENT_LOCK.equals(ROOT_LOCK + "/" + lockObjects.get(0))) {
                return true;
            }

            // 如果不是最小的节点，找到自己之前的那个节点
            String prevNode = CURRENT_LOCK.substring(CURRENT_LOCK.lastIndexOf("/")+1);
            WAIT_LOCK = lockObjects.get(Collections.binarySearch(lockObjects, prevNode)-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

        return false;
    }

    // 等待锁
    private boolean waitForLock(String prev, long waitTime) throws InterruptedException, KeeperException {
        Stat stat = zooKeeper.exists(ROOT_LOCK + "/" + prev, true);

        if (stat != null) {
            System.out.println(Thread.currentThread().getName() + "等待锁 " + ROOT_LOCK + "/" + prev);
            this.countDownLatch = new CountDownLatch(1);
            // 计数等待，若等到前一个节点消失，则precess中进行countDown，停止等待，获取锁
            this.countDownLatch.await(waitTime, TimeUnit.MILLISECONDS);
            this.countDownLatch = null;
            System.out.println(Thread.currentThread().getName() + " 等到了锁");
        }

        return true;
    }

    @Override
    public void unlock(String lockname) {
        try {
            System.out.println("释放锁！" + CURRENT_LOCK);
            zooKeeper.delete(CURRENT_LOCK, -1);
            CURRENT_LOCK = null;
            zooKeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void process(WatchedEvent watchedEvent) {
        if (this.countDownLatch != null) {
            this.countDownLatch.countDown();
        }
    }

    public class LockException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public LockException(String e){
            super(e);
        }
        public LockException(Exception e){
            super(e);
        }
    }
}
