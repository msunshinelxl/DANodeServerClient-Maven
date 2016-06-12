package com.agilor.distribute.testZK;

import com.agilor.distribute.common.Constant;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMultiLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by xinlongli on 16/5/30.
 */
public class zkClientTest {
    public static final String connectStr= Constant.ZK_IP+":"+Constant.ZK_PORT;
    CuratorFramework zkClient;
    private InterProcessMutex lock=null;
    final InterProcessMultiLock multiLock=null;
    private InterProcessReadWriteLock readWriteLock=null;
    CountDownLatch mythreadCount=new CountDownLatch(0);
    public zkClientTest(String connectStr) throws Exception{
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,3);
        zkClient=CuratorFrameworkFactory
                .builder()
                .namespace("zktest")
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(3000)
                .connectString(connectStr)
                .retryPolicy(retryPolicy)
                .build();
        zkClient.start();
        //zkClient.create().forPath("/mutex_test");
        lock=new InterProcessMutex(zkClient,"/mutex_test");
        readWriteLock=new InterProcessReadWriteLock(zkClient,"/readWrite_lock");
        long st=System.currentTimeMillis();
        for(int i=0;i<1;i++){
            readWriteLock.writeLock().acquire();
            System.out.println("i'm here");
            Thread.sleep(Integer.MAX_VALUE);
            //readWriteLock.readLock().release();
        }
        System.out.println(System.currentTimeMillis()-st);

    }
    public static void main(String [] args){
        try{
            zkClientTest test=new zkClientTest(connectStr);
            Thread.sleep(Integer.MAX_VALUE);
        }catch(Exception e){
            e.printStackTrace();
        }

    }

}
