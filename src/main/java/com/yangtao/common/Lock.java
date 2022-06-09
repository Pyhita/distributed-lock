package com.yangtao.common;

/**
 * @Author: pyhita
 * @Date: 2022/6/7
 * @Descrption: com.yangtao.common
 * @Version: 1.0
 */
public interface Lock {
    // 获取锁
    void getLock(String lockname) throws InterruptedException;

    // 释放锁
    void unlock(String lockname);
}
