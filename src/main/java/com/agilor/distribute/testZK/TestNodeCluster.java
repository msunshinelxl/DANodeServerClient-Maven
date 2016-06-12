package com.agilor.distribute.testZK;

import com.agilor.distribute.common.Constant;
import com.agilor.distribute.consistenthash.ConsistentHash;
import com.agilor.distribute.consistenthash.MD5Hash;
import com.agilor.distribute.consistenthash.Node;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

/**
 * Created by xinlongli on 16/5/26.
 */
public class TestNodeCluster {
    CuratorFramework zkClient;
    HashMap<String,List<DataThread>> excutorMap;
    ConsistentHash nodeList;

    public TestNodeCluster(String connectStr,String namespace){
        excutorMap=new HashMap<String,List<DataThread>>();

        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,3);
        zkClient= CuratorFrameworkFactory.builder()
                .namespace(namespace)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(3000)
                .connectString(connectStr)
                .retryPolicy(retryPolicy)
                .build();
        zkClient.start();
        nodeList = new ConsistentHash(new MD5Hash());
    }




    /**
     * init dynamic folder
     * */
    private void initDynamicPath(int nodeID,String content) throws Exception{
        String myDynamicPath=Constant.zkDynamicPath+"/"+nodeID;
        Stat dynamicFolderState=zkClient.checkExists()
                .creatingParentContainersIfNeeded()
                .forPath(myDynamicPath);
        if(dynamicFolderState!=null){
            if(dynamicFolderState.getNumChildren()>0){
                List<String> children=zkClient.getChildren().forPath(myDynamicPath);
                boolean hasStatPath=false;
                for(int i=0;i<children.size();i++){
                    if(!children.get(i).endsWith(Constant.zkStatSubPath)){
                        try{
                            zkClient.delete().forPath(myDynamicPath+"/"+children.get(i));
                        }catch(Exception e){
                            e.printStackTrace();
                        }
                    }else{
                        hasStatPath=true;
                    }
                }
                if(!hasStatPath){
                    zkClient.create()
                            .creatingParentsIfNeeded()
                            .withMode(CreateMode.PERSISTENT)
                            .forPath(myDynamicPath+Constant.zkStatSubPath);
                }
            }
        }else{
            zkClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(myDynamicPath+Constant.zkStatSubPath);
        }
        zkClient.setData().forPath(myDynamicPath + Constant.zkStatSubPath, content.getBytes());
    }

    private void stopAllTask(String id){
        System.err.println("start stopping "+id);
        if(!excutorMap.containsKey(id)){
            return;
        }
        System.err.println("stopping " + id);
        excutorMap.get(id).forEach((DataThread thread) -> {
            try {
                thread.myStop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        excutorMap.remove(id);
    }

    private void addTask(String id,DataThread thread){
        if(!excutorMap.containsKey(id)){
            excutorMap.put(id,new ArrayList<>());
        }
        excutorMap.get(id).add(thread);
        thread.start();
    }

    private void updateNodeList() throws Exception{
        nodeList.clear();
        List<String> children=zkClient.getChildren().forPath(Constant.zkNodePath);
        for(String path:children){
            String tmpData=new String(zkClient.getData().forPath(Constant.zkNodePath+"/"+path));
            nodeList.add(new Node(new JSONObject(tmpData)));
        }
    }

    /**
     * init node server,including:
     * 1.get other nodes info
     * 2.create my node info
     * 3.monitor other node,data is migrate to my node
     * 4.redirect client's new data to new node and old node while initialization
     *
     * @param myNode new node info
     * @return 0   OK
     *         -1  node id duplicate
     *
     * */
    public int clusterMonitor(Node myNode) throws Exception{
        Stat nodeStat=zkClient.checkExists().creatingParentContainersIfNeeded().forPath(Constant.zkNodePath);
        if(nodeStat!=null){
            List<String> childrenList=zkClient.getChildren().forPath(Constant.zkNodePath);
            if(childrenList!=null){
                childrenList.forEach((String item)->{
                    /**
                     * init cluster info
                     * */
                    try{
                        JSONObject data=new JSONObject(new String(zkClient.getData().forPath(Constant.zkNodePath+"/"+item)));

                    }catch(Exception e){
                        e.printStackTrace();
                    }
                });
            }
        }else{
            zkClient.create().creatingParentsIfNeeded()
            .forPath(Constant.zkNodePath);
        }


        initDynamicPath(myNode.getId(),"ADD");
        /**
         * notify client to block themselves
         * */

        /**
         * path has already existed
         * */
        final String zkClientPath=Constant.zkClientPath+"/"+myNode.getId();
        if(zkClient.checkExists().forPath(zkClientPath)==null){
            zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(zkClientPath);
        }else{
            System.err.println("Client Path "+zkClientPath+" exists");
        }
        /**
         * init myNode,like create tagName
         * */




        PathChildrenCache nodeCache=new PathChildrenCache(zkClient,Constant.zkNodePath,true);
        nodeCache.getListenable().addListener(
                (CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) -> {
                    String []subPath=pathChildrenCacheEvent
                            .getData().getPath().split("/");
                    final String tmpID=subPath[subPath.length-1];
                    /**
                     * update node info
                     * */
                    updateNodeList();
                    switch (pathChildrenCacheEvent.getType()) {
                        case CHILD_ADDED: {
                            /**
                             * 1 stop block client when finishing adding,control the progress of add node
                             * 2 other node countdown the numbers
                             * */
                            Stat tmpParents=zkClient
                                    .checkExists()
                                    .creatingParentContainersIfNeeded()
                                    .forPath(Constant.zkNodePath);
                            int nodeChildrenNums=tmpParents.getNumChildren();

                            final String dynamicBarrier=Constant.zkDynamicPath+"/"
                                    +tmpID+"/barrier";
                            final DistributedDoubleBarrier barrier
                                    =new DistributedDoubleBarrier(zkClient,dynamicBarrier,nodeChildrenNums);
                            if(tmpID.compareTo(String.valueOf(myNode.getId()))==0){
                                barrier.enter();
                                barrier.leave();
                                System.out.println("finished migrate "+tmpID);
                                zkClient.delete().forPath(zkClientPath);
                                zkClient.delete().deletingChildrenIfNeeded().forPath(Constant.zkDynamicPath+"/"
                                        +subPath[subPath.length-1]);
                            }else{
                                stopAllTask(String.valueOf(tmpID));
                                addTask(String.valueOf(tmpID),new DataThread(barrier, new DataThread.Runner() {
                                    @Override
                                    public void beforRun(DataThread pa) {

                                    }

                                    @Override
                                    public void running(DataThread pa) {
                                        try{
                                            System.out.println("migrating to "+tmpID);
                                            Random ran=new Random();

                                            for(int i=0;i<10000&&pa.stat;i++){
                                                Thread.sleep(ran.nextInt(10));
                                            }
                                            System.out.println("migrating to "+tmpID);
                                            if(!pa.stat){
                                                System.err.println("migrate STOP! "+tmpID);
                                            }
                                        }catch(Exception e){
                                            e.printStackTrace();
                                        }
                                    }

                                    @Override
                                    public void afterRun(DataThread pa) {

                                    }
                                }));
                            }
                            break;
                        }
                        case CHILD_REMOVED:
                            stopAllTask(String.valueOf(tmpID));
                            Stat tmpParents=zkClient
                                    .checkExists()
                                    .creatingParentContainersIfNeeded()
                                    .forPath(Constant.zkNodePath);
                            int nodeChildrenNums=tmpParents.getNumChildren();
                            final String dynamicBarrier=Constant.zkDynamicPath+"/"
                                    +tmpID+"/barrier";
                            final DistributedDoubleBarrier barrier
                                    =new DistributedDoubleBarrier(zkClient,dynamicBarrier,nodeChildrenNums);

                            Node tmpMainNode=nodeList.getNextNode(new Node(Integer.valueOf(tmpID)));
                            addTask(tmpID,new DataThread(barrier, new DataThread.Runner() {
                                @Override
                                public void beforRun(DataThread pa) {
                                    if(myNode.getId()==tmpMainNode.getId()){
                                        try{
                                            if(zkClient.checkExists().forPath(zkClientPath)==null){
                                                zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(zkClientPath);
                                            }else{
                                                System.err.println("Client Path "+zkClientPath+" exists");
                                            }
                                        }catch(Exception e){
                                            e.printStackTrace();
                                        }
                                    }
                                }

                                @Override
                                public void running(DataThread pa) {
                                    try{
                                        System.out.println("adding Tag to "+myNode.getId());
                                        Random ran=new Random();
                                        for(int i=0;i<500&&pa.stat;i++){
                                            Thread.sleep(ran.nextInt(10));
                                        }
                                        if(!pa.stat){
                                            System.err.println("add STOP! "+myNode.getId());
                                        }
                                    }catch(Exception e){
                                        e.printStackTrace();
                                    }
                                }

                                @Override
                                public void afterRun(DataThread pa) {
                                    if(myNode.getId()==tmpMainNode.getId()){
                                        try{
                                            zkClient.delete().forPath(zkClientPath);
                                            zkClient.delete().deletingChildrenIfNeeded().forPath(Constant.zkDynamicPath+"/"
                                                    +subPath[subPath.length-1]);
                                        }catch(Exception e){
                                            e.printStackTrace();
                                        }
                                    }
                                }
                            }));
                            break;
                    }
                });
        nodeCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        //zkClient.getChildren().forPath(Constant.zkNodePath);
        Stat tmpIDStat=zkClient.checkExists().forPath(Constant.zkNodePath + "/" + myNode.getId());
        if(tmpIDStat!=null){
            return -1;
        }
        zkClient.create().creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(Constant.zkNodePath + "/" + myNode.getId(), new JSONObject(myNode.nodeToMap()).toString().getBytes());

        return 0;
    }

    public static void main(String [] args){
        TestNodeCluster test=new TestNodeCluster( Constant.ZK_IP+":"+Constant.ZK_PORT,"zktest1");
        try{
            test.clusterMonitor(new Node("0.0.0.2",30,"node2",2));
            Thread.sleep(Integer.MAX_VALUE);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
