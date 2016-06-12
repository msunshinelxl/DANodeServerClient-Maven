package com.agilor.distribute.client.nameManage;

import java.util.*;
import java.util.Map.Entry;

import agilor.distributed.communication.client.Client;
import agilor.distributed.communication.client.Value;
import agilor.distributed.communication.protocol.SimpleProtocol;
import agilor.distributed.communication.result.AddValueResultFuture;
import agilor.distributed.communication.result.ResultFuture;
import agilor.distributed.communication.socket.Connection;
import com.agilor.distribute.common.ComFuncs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.agilor.distribute.common.Constant;
import com.agilor.distribute.consistenthash.NodeDevice;
import com.agilor.distribute.test.LogTestMain;

public class AgilorDistributeClient {


	private class DistributeInfo {
		NodeDevice main = null;
		NodeDevice tmp = null;
	}
	
	private interface DistributeLogInterface{
		ResultFuture mainNodeCallBack(Client agilor) throws Exception;
		ResultFuture tmpNodeCallBack(Client agilor) throws Exception;
	}
	
	
	final static Logger logger = LoggerFactory.getLogger(LogTestMain.class);
	Map<String,Client>activityAgilor;
	public AgilorDistributeClient() {
		activityAgilor=new HashMap<String, Client>();
	}

//	public Agilor openSession

	/**
	 * 该函数创建数据点，分为主备两个创建数据点
	 * @param tagName 点名
	 * @param val 数据初始化内容
	 * @return null 表示出现异常或者未初始化Agilor
	 *         List第一个元素为主点添加返回值，后面为备份节点返回值，错误信息会反映在各元素的errorCode字段上,0表示OK
	 *
	 * */
	public List<ResultFuture> createTagNode(final String tagName,final Value val) {
		final DistributeInfo distributeInfo = getDistributeInfo(tagName);
		try {
			return distributeLogFrame(tagName,distributeInfo,new DistributeLogInterface(){

				public ResultFuture mainNodeCallBack(Client agilor) throws Exception {
					// TODO Auto-generated method stub
					return ComFuncs.createTag(agilor, tagName, distributeInfo.main.getDevice(), logger,val);
//					if (res == null){
//						logger.error("create failed :"
//								+ distributeInfo.main.getNode().getIp() + " : "
//								+ distributeInfo.main.getDevice() + " : "
//								+ tagName);
////					}
//					return res;
				}
				@Override
				public ResultFuture tmpNodeCallBack(Client agilor) throws Exception{
					// TODO Auto-generated method stub
					return ComFuncs.createTag(agilor, tagName, distributeInfo.main.getDevice(), logger,val);
//					if (res == null){
//						logger.error("create failed :"
//								+ distributeInfo.tmp.getNode().getIp() + " : "
//								+ distributeInfo.tmp.getDevice() + " : "
//								+ tagName);
//					}
//					return res;
				}
				
			});
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	/**
	 * 数据写入
	 * @param tagName 点名
	 * @param value 点值
	 * @param flush 是否立即写入，立即写入会影响整体IO
	 * @return null 表示出现异常或者未初始化Agilor
	 *         List第一个元素为主点添加返回值，后面为备份节点返回值，错误信息会反映在各元素的errorCode字段上,0表示OK
	 *
	 * */
	public List<ResultFuture> write(final String tagName,final Value value,final boolean flush){
		DistributeInfo distributeInfo=getDistributeInfo(tagName);
		try {
			
			return distributeLogFrame(tagName,distributeInfo,new DistributeLogInterface(){


				public ResultFuture mainNodeCallBack(Client agilor) throws Exception{
					// TODO Auto-generated method stub
					return ComFuncs.writeTagValue(agilor, tagName, value, distributeInfo.main.getDevice(),flush);
				}
				public ResultFuture tmpNodeCallBack(Client agilor) throws Exception{
					// TODO Auto-generated method stub
					return ComFuncs.writeTagValue(agilor, tagName, value ,distributeInfo.tmp.getDevice(),flush);
				}
			});
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	public void close(){
		Iterator<Entry<String, Client>> it = activityAgilor.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<String,Client> pair = it.next();
	        try {
				pair.getValue().close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.error("close failed :"
						+e.toString());
			}
	    }
	    activityAgilor.clear();
	}
	
	// private method *********************************************************
	private Client getAgilor(NodeDevice disInfo){
		String keyName=disInfo.getNode().getIp();
		if(activityAgilor.containsKey(keyName)){
			return activityAgilor.get(keyName);
		}else{
			try {
				Client tmpAgilor=new Client(new Connection(disInfo.getNode().getIp(), Constant.agilorServerPort, SimpleProtocol.getInstance()));
				tmpAgilor.open();
				activityAgilor.put(keyName, tmpAgilor);
				return tmpAgilor;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("create Agilor failed : ip: "+keyName+" "+e.toString());
				return null;
			}
		}
	}
	
	
	private List<ResultFuture> distributeLogFrame(String tagName,DistributeInfo distributeInfo,DistributeLogInterface callback){
		List<ResultFuture> result=new ArrayList<>();
		if (distributeInfo.main != null) {
			try {
				Client agilor=getAgilor(distributeInfo.main);
				if(agilor==null){
					return null;
				}
				ResultFuture mainRes=callback.mainNodeCallBack(agilor);
				if(mainRes==null){
					result.add(new AddValueResultFuture(Constant.ERROR_FROM_AGILOR));
				}else{
					result.add(mainRes);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		} else {
			logger.error("create failed : NodeDevice Main is null " + " : "
					+ tagName);
			result.add(new AddValueResultFuture(Constant.ERROR_DISTRIBUTION_INFO));
		}
		if (distributeInfo.tmp != null) {
			try {
				Client agilor=getAgilor(distributeInfo.tmp);
				if(agilor==null){
					return null;
				}
				logger.info("create : NodeDevice Tmp is created " + " : "
						+ tagName);
				ResultFuture tmpRes=callback.mainNodeCallBack(agilor);
				if(tmpRes==null){
					result.add(new AddValueResultFuture(Constant.ERROR_FROM_AGILOR));
				}else{
					result.add(tmpRes);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}else{
			result.add(new AddValueResultFuture(Constant.ERROR_DISTRIBUTION_INFO));
		}
		return result;
	}
	


	private DistributeInfo getDistributeInfo(String tagName) {
		NodeDevice distributeInfoFinal = null;
		NodeDevice distributeInfoTmp = null;
		ClientNodeHandler nodeInfo = ClientNodeHandler.getClientNodeHandler();
		if (nodeInfo != null && nodeInfo.finalNodeList != null) {
			distributeInfoFinal = nodeInfo.finalNodeList.get(tagName);
		} else {
			if(nodeInfo==null)
				logger.error("getDistributeInfo error: ClientNodeHandler.getClientNodeHandler() null");
			if(nodeInfo.finalNodeList==null){
				logger.error("getDistributeInfo.finalNodeList error: null");
			}
		}

		if (nodeInfo != null && nodeInfo.tmpNodeList != null) {
			distributeInfoTmp = nodeInfo.tmpNodeList.get(tagName);
		} else {
			if(nodeInfo.tmpNodeList==null){
				logger.error("getDistributeInfo.tmpNodeList error: null");
			}
		}
		DistributeInfo res = new DistributeInfo();
		res.main = distributeInfoFinal;
		res.tmp = distributeInfoTmp;
		return res;
	}


	
}
