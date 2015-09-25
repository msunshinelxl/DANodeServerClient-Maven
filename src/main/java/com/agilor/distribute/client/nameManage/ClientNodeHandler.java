package com.agilor.distribute.client.nameManage;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.zookeeper.KeeperException;
import org.json.JSONArray;
import org.json.JSONObject;

import com.agilor.distribute.common.Constant;
import com.agilor.distribute.common.Interface.ZnodeGetDataCallback;
import com.agilor.distribute.consistenthash.ConsistentHash;
import com.agilor.distribute.consistenthash.MD5Hash;
import com.agilor.distribute.consistenthash.Node;
import com.agilor.distribute.zookeeper.Executor;

public class ClientNodeHandler {
	private String hostPort;
	private Executor taskExecutor;
	private ZnodeGetDataCallback taskCallback;
	private String znodeClientInfo;
	public ConsistentHash finalNodeList;
	public ConsistentHash tmpNodeList;
//	public ReentrantReadWriteLock finalNodeListLock;
//	public ReentrantReadWriteLock tmpNodeListLock;
	private static volatile ClientNodeHandler singlor;
	private static volatile boolean isFinishInit ;
	public static ClientNodeHandler getClientNodeHandler(){
		if(singlor==null){
			synchronized (ClientNodeHandler.class) {
				if(singlor==null){
					singlor=new ClientNodeHandler();
					synchronized (singlor) {
						try {
							isFinishInit=false;
							singlor.stratHandler();
							singlor.wait();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}
		}
		return singlor;
	}
	
	private ClientNodeHandler(){
		init("101.200.77.14:2181","/ClientNodeInfo");
	}
	private void init(String hostPort,
			String znodeClientInfo){
		this.hostPort=hostPort;
		this.znodeClientInfo=znodeClientInfo;
		try {
			taskExecutor = new Executor(hostPort, Constant.zRootNode
					+ znodeClientInfo,Constant.zkTimeLong);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			System.err.println("taskExecutor in NodeHandler KeeperException");
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("taskExecutor in NodeHandler IOException");
			e.printStackTrace();
		}
		finalNodeList = new ConsistentHash(new MD5Hash());
		tmpNodeList=new ConsistentHash(new MD5Hash());
	}
	private ClientNodeHandler(String hostPort,
			String znodeClientInfo) {
		init(hostPort,znodeClientInfo);
	}
	private void stratHandler() {
		taskCallback = new ZnodeGetDataCallback() {

			@Override
			public void todo(Executor mine, byte[] data) {
				// TODO Auto-generated method stub
				JSONObject receiveJo = new JSONObject(new String(data));
				if(receiveJo.has(Constant.zkNodeClientFinalListName)){
					JSONArray receiveFinalJa = receiveJo.getJSONArray(Constant.zkNodeClientFinalListName);
//					finalNodeListLock.writeLock().lock();
					System.out.println(receiveFinalJa.toString());
					finalNodeList.clear();
					for(int i=0;i<receiveFinalJa.length();i++){
						finalNodeList.add(new Node(receiveFinalJa.getJSONObject(i)));
					}
					if(finalNodeList!=null&&finalNodeList.getNodes()!=null)
						System.out.println(finalNodeList.getNodes().size());
//					finalNodeListLock.writeLock().unlock();
				}
				if(receiveJo.has(Constant.zkNodeClientTmpListName)){
					JSONArray receiveTmpJa = receiveJo.getJSONArray(Constant.zkNodeClientTmpListName);
					
//					tmpNodeListLock.writeLock().lock();
					System.out.println(receiveTmpJa.toString());
					tmpNodeList.clear();
					for(int i=0;i<receiveTmpJa.length();i++){
						tmpNodeList.add(new Node(receiveTmpJa.getJSONObject(i)));
					}
					if(tmpNodeList!=null&&tmpNodeList.getNodes()!=null)
						System.out.println(tmpNodeList.getNodes().size());
//					tmpNodeListLock.writeLock().unlock();
				}
				
				if(isFinishInit==false){
					synchronized(singlor){
						singlor.notifyAll();
						isFinishInit=true;
					}
				}
			}

			@Override
			public void onClose(Executor parent) {
				// TODO Auto-generated method stub
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					taskExecutor = new Executor(hostPort, Constant.zRootNode
							+ znodeClientInfo,Constant.zkTimeLong);
					parent.zk.close();
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}catch (InterruptedException e) {
					e.printStackTrace();
				}

				stratHandler();
			}
			
		};
		taskExecutor.setGetdataCallback(taskCallback);
		taskExecutor.start();
	}
}
