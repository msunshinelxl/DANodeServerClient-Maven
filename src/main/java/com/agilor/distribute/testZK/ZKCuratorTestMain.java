package com.agilor.distribute.testZK;

import com.agilor.distribute.common.Constant;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by xinlongli on 16/5/25.
 */
public class ZKCuratorTestMain {
    CuratorFramework zkClient=null;
    public static final String connectStr= Constant.ZK_IP+":"+Constant.ZK_PORT;
    CountDownLatch tCount=new CountDownLatch(1);
    ZKCuratorTestMain(){
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,3);
        zkClient=CuratorFrameworkFactory
                .builder()
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(3000)
                .connectString(connectStr)
                .retryPolicy(retryPolicy)
                .build();
        zkClient.start();
    }

    public void connectTest(String pathStr){
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
        try{
            //zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(pathStr);
            final NodeCache listener=new NodeCache(zkClient,pathStr,false);
            listener.start(true);
            NodeCacheListener tmpListener=new NodeCacheListener(){

                @Override
                public void nodeChanged() throws Exception {
                    if (listener.getCurrentData() != null && listener.getCurrentData().getData() != null) {
                        System.out.println(new String(listener.getCurrentData().getData()));
                        Thread.sleep(1000);
                    }
                    else {
                        System.out.println("error!");
                        tCount.countDown();
                    }
                }
            };
            Stat childrens=zkClient.checkExists().forPath("/c1");
            if(childrens!=null){
                System.out.println(childrens);
            }
            listener.getListenable().addListener(tmpListener);
            zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(pathStr);
            zkClient.setData().forPath(pathStr, "hah".getBytes());

            Stat lal=zkClient.setData().forPath(pathStr, "lal".getBytes());
            System.out.println(lal.getVersion());
            //Thread.sleep(1000);
            //zkClient.delete().forPath(pathStr);
            tCount.await();
            listener.getListenable().removeListener(tmpListener);
            Thread.sleep(Integer.MAX_VALUE);
        }catch(Exception e){
            e.printStackTrace();
        }

    }

    public void childConTest(String pathStr) throws Exception{
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
        PathChildrenCache cache=new PathChildrenCache(zkClient,pathStr,true);
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                switch (pathChildrenCacheEvent.getType()){
                    case CHILD_ADDED:
                        System.out.println("ADD "+pathChildrenCacheEvent.getData());
                        break;
                    case CHILD_REMOVED:
                        System.out.println("REMOVE "+pathChildrenCacheEvent.getData());
                        break;
                    case CHILD_UPDATED:
                        System.out.println("UPDATE "+pathChildrenCacheEvent.getData());
                        break;
                }
            }
        });
        cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        zkClient.create()
                .creatingParentContainersIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(pathStr + "/c1", "hah".getBytes());

        zkClient.setData().forPath(pathStr + "/c1", "lal".getBytes());
        Thread.sleep(5000);
        zkClient.delete().forPath(pathStr + "/c1");
        zkClient.delete().forPath(pathStr);
        Thread.sleep(Integer.MAX_VALUE);
    }

    public void clusterTest(String nodepath) throws Exception{
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,3);
        zkClient=CuratorFrameworkFactory
                .builder()
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(3000)
                .connectString(connectStr)
                .retryPolicy(retryPolicy)
                .build();
        zkClient.start();
        Stat nodeStat=zkClient.checkExists().forPath(nodepath);
        if(nodeStat!=null){
            List<String> childrenList=zkClient.getChildren().forPath(nodepath);
            if(childrenList!=null){
                childrenList.forEach((String item)->{
                    /**
                     * init cluster info
                     * */
                    try{
                        //JSONObject data=new JSONObject(zkClient.getData().forPath(item));
                        System.out.println("path : "+item);
                        JSONObject jo=new JSONObject(new String(zkClient.getData().forPath(nodepath+"/"+item)));

                        System.out.println("content : "+jo.getString("name"));
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                });
            }
        }else{
            System.out.println(nodepath+" not found");
        }
        Thread.sleep(Integer.MAX_VALUE);
    }

    public void testBarrier() throws Exception{
        int nodeChildrenNums=1;
        final String dynamicBarrier=Constant.zkDynamicPath+"/"
                +"hah"+"/barrier";
        final DistributedDoubleBarrier barrier
                =new DistributedDoubleBarrier(zkClient,dynamicBarrier,nodeChildrenNums);
        barrier.enter();

        barrier.leave();
        zkClient.delete().forPath(dynamicBarrier);
    }


    public static void main(String[] args){
        ZKCuratorTestMain test=new ZKCuratorTestMain();
        try{
            test.testBarrier();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
