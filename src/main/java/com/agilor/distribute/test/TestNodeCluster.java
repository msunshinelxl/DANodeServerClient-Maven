package com.agilor.distribute.test;

import com.agilor.distribute.common.Constant;
import com.agilor.distribute.consistenthash.Node;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;

import java.util.List;

/**
 * Created by xinlongli on 16/5/26.
 */
public class TestNodeCluster {
    CuratorFramework zkClient;

    public TestNodeCluster(String connectStr,String namespace){
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,3);
        zkClient= CuratorFrameworkFactory.builder()
                .namespace(namespace)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(3000)
                .connectString(connectStr)
                .retryPolicy(retryPolicy)
                .build();
        zkClient.start();
    }

    /**
     * init node server,including:
     * 1.get other nodes info
     * 2.create my node info
     * 3.data is migrate to my node
     * 4.redirect client's new data to new node and old node while initialization
     *
     * @param myNode new node info
     * @return 0   OK
     *         -1  node id duplicate
     *
     * */
    public int clusterMonitor(Node myNode) throws Exception{
        Stat nodeStat=zkClient.checkExists().forPath(Constant.zkNodePath);
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
        }
        //zkClient.getChildren().forPath(Constant.zkNodePath);
        zkClient.checkExists().forPath(Constant.zkNodePath + "/" + myNode.getId());

        zkClient.create().creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(Constant.zkNodePath + "/" + myNode.getId(), myNode.toString().getBytes());

        return 0;
    }

    public static void main(String [] args){
        TestNodeCluster test=new TestNodeCluster( Constant.ZK_IP+":"+Constant.ZK_PORT,"zktest1");
        try{
            Thread.sleep(Integer.MAX_VALUE);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
