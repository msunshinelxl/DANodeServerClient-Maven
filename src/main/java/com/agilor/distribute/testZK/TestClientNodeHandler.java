package com.agilor.distribute.testZK;

import com.agilor.distribute.common.Constant;
import com.agilor.distribute.common.Interface.ZnodeGetDataCallback;
import com.agilor.distribute.consistenthash.ConsistentHash;
import com.agilor.distribute.consistenthash.MD5Hash;
import com.agilor.distribute.consistenthash.Node;
import com.agilor.distribute.zookeeper.Executor;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestClientNodeHandler {
	private String hostPort;
	private String znodeClientInfo;
	public ConsistentHash finalNodeList;
	public List<ConsistentHash> backupNodeList;
	private CountLatch countDown;
    private CuratorFramework zkClient;
//	public ReentrantReadWriteLock finalNodeListLock;
//	public ReentrantReadWriteLock tmpNodeListLock;
	private static volatile TestClientNodeHandler singlor;
	private static volatile boolean isFinishInit ;
	public static TestClientNodeHandler getClientNodeHandler(String connectStr,String namespace){
		if(singlor==null){
			synchronized (TestClientNodeHandler.class) {
				if(singlor==null){
                    try {
                        singlor=new TestClientNodeHandler(connectStr,namespace);
					    synchronized (singlor) {
                                singlor.updateNodeInfo();
                            }
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

				}
			}
		}
		return singlor;
	}

	private TestClientNodeHandler(String connectStr,String namespace)throws Exception{
		init(connectStr,namespace);
	}
	private void init (String conStr,
			String namespace)throws Exception{
        countDown=new CountLatch(0);
        finalNodeList = new ConsistentHash(new MD5Hash());
        backupNodeList=new ArrayList<>();
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,3);
        zkClient= CuratorFrameworkFactory.builder()
                .namespace(namespace)
                .sessionTimeoutMs(5000)
                .connectionTimeoutMs(3000)
                .connectString(conStr)
                .retryPolicy(retryPolicy)
                .build();
        zkClient.start();
        PathChildrenCache nodeCache=new PathChildrenCache(zkClient,Constant.zkClientPath,true);
        nodeCache.getListenable().addListener((CuratorFramework curatorFramework,
                                               PathChildrenCacheEvent pathChildrenCacheEvent) -> {
            switch (pathChildrenCacheEvent.getType()) {
                case CHILD_ADDED: {
                    System.out.println(pathChildrenCacheEvent.getData().getPath());
                    countDown.countAdd();
                    break;
                }
                case CHILD_REMOVED: {
                    countDown.countDown();
                    break;
                }
            }
        });

        nodeCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        countDown.setAfterWait(()-> {
            try{
                updateNodeInfo();
            }catch (Exception e){
                e.printStackTrace();
            }
        });
	}

    public void synchronizeNodeInfo() throws InterruptedException{
        countDown.await();
    }
    /**
     * update node info
     * */
    public void updateNodeInfo()throws Exception{
        List<String> paths=zkClient.getChildren().forPath(Constant.zkNodePath);
        finalNodeList.clear();
        for(String path:paths){
            String tmpStr=new String(zkClient.getData().forPath(Constant.zkNodePath+"/"+path));
            finalNodeList.add(new Node(new JSONObject(tmpStr)));
        }
        finalNodeList.showContent();
        System.out.println("*****************");
    }
//	private void stratHandler() {
//		taskCallback = new ZnodeGetDataCallback() {
//
//			@Override
//			public void todo(Executor mine, byte[] data) {
//				// TODO Auto-generated method stub
//				JSONObject receiveJo = new JSONObject(new String(data));
//				if(receiveJo.has(Constant.zkNodeClientFinalListName)){
//					JSONArray receiveFinalJa = receiveJo.getJSONArray(Constant.zkNodeClientFinalListName);
////					finalNodeListLock.writeLock().lock();
//					System.out.println(receiveFinalJa.toString());
//					finalNodeList.clear();
//					for(int i=0;i<receiveFinalJa.length();i++){
//						finalNodeList.add(new Node(receiveFinalJa.getJSONObject(i)));
//					}
//					if(finalNodeList!=null&&finalNodeList.getNodes()!=null)
//						System.out.println(finalNodeList.getNodes().size());
////					finalNodeListLock.writeLock().unlock();
//				}
//				if(receiveJo.has(Constant.zkNodeClientTmpListName)){
//					JSONArray receiveTmpJa = receiveJo.getJSONArray(Constant.zkNodeClientTmpListName);
//
////					tmpNodeListLock.writeLock().lock();
//					System.out.println(receiveTmpJa.toString());
//					tmpNodeList.clear();
//					for(int i=0;i<receiveTmpJa.length();i++){
//						tmpNodeList.add(new Node(receiveTmpJa.getJSONObject(i)));
//					}
//					if(tmpNodeList!=null&&tmpNodeList.getNodes()!=null)
//						System.out.println(tmpNodeList.getNodes().size());
////					tmpNodeListLock.writeLock().unlock();
//				}
//
//				if(isFinishInit==false){
//					synchronized(singlor){
//						singlor.notifyAll();
//						isFinishInit=true;
//					}
//				}
//			}
//
//			@Override
//			public void onClose(Executor parent) {
//				// TODO Auto-generated method stub
//				try {
//					Thread.sleep(1000);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//				try {
//					taskExecutor = new Executor(hostPort, Constant.zkRootPath
//							+ znodeClientInfo,Constant.zkTimeLong);
//					parent.zk.close();
//				} catch (KeeperException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//
//				stratHandler();
//			}
//
//		};
//		taskExecutor.setGetdataCallback(taskCallback);
//		taskExecutor.start();
//	}
}
