package com.agilor.distribute.client.nameManage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import agilor.distributed.communication.client.Client;
import agilor.distributed.communication.client.Value;
import agilor.distributed.communication.protocol.SimpleProtocol;
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
		int mainNodeCallBack(Client agilor) throws Exception;
		int tmpNodeCallBack(Client agilor) throws Exception;
	}
	
	
	final static Logger logger = LoggerFactory.getLogger(LogTestMain.class);
	Map<String,Client>activityAgilor;
	public AgilorDistributeClient() {
		activityAgilor=new HashMap<String, Client>();
	}

//	public Agilor openSession
	public int createTagNode(String tagName,Value val) {
		DistributeInfo distributeInfo = getDistributeInfo(tagName);
		try {
			return distributeLogFrame(tagName,distributeInfo,new DistributeLogInterface(){

				@Override
				public int mainNodeCallBack(Client agilor) throws Exception {
					// TODO Auto-generated method stub
					if (ComFuncs.createTag(agilor, tagName, distributeInfo.main.getDevice(), logger,val) == false){
						logger.error("create failed :"
								+ distributeInfo.main.getNode().getIp() + " : "
								+ distributeInfo.main.getDevice() + " : "
								+ tagName);
						return Constant.ERROR_FROM_AGILOR;
					}
					return Constant.SUCESS;

				}

				@Override
				public int tmpNodeCallBack(Client agilor) throws Exception{
					// TODO Auto-generated method stub
					if (ComFuncs.createTag(agilor, tagName, distributeInfo.tmp.getDevice(),logger,val)==false){
						logger.error("create failed :"
								+ distributeInfo.tmp.getNode().getIp() + " : "
								+ distributeInfo.tmp.getDevice() + " : "
								+ tagName);
						return Constant.ERROR_FROM_AGILOR;
					}
					return Constant.SUCESS;
				}
				
			});
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return Constant.ERROR_DEFAULT;
		}
	}

	public int write(String tagName,Value value){
		DistributeInfo distributeInfo=getDistributeInfo(tagName);
		try {
			
			return distributeLogFrame(tagName,distributeInfo,new DistributeLogInterface(){

				@Override
				public int mainNodeCallBack(Client agilor) throws Exception{
					// TODO Auto-generated method stub
					ComFuncs.writeTagValue(agilor, tagName, value,logger, distributeInfo.main.getDevice());
					return Constant.SUCESS;
				}

				@Override
				public int tmpNodeCallBack(Client agilor) throws Exception{
					// TODO Auto-generated method stub
					ComFuncs.writeTagValue(agilor, tagName, value ,logger,distributeInfo.tmp.getDevice());
					return Constant.SUCESS;
				}
			});
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return Constant.ERROR_FROM_AGILOR;
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
				Client tmpAgilor=new Client(new Connection(disInfo.getNode().getIp(), Constant.agilorServerPort, Constant.agilorNodeServerTimeout, SimpleProtocol.getInstance()));
				tmpAgilor.open();
				activityAgilor.put(keyName, tmpAgilor);
				return tmpAgilor;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.error("create Agilor failed : "+e.toString());
				return null;
			}
		}
	}
	
	
	private int distributeLogFrame(String tagName,DistributeInfo distributeInfo,DistributeLogInterface callback){
		int result=0;
		if (distributeInfo.main != null) {
			try {
				Client agilor=getAgilor(distributeInfo.main);
				if(agilor==null){
					return Constant.ERROR_AGILORINI_FAIL;
				}
				result=callback.mainNodeCallBack(agilor);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return Constant.ERROR_DEFAULT;
			}
		} else {
			logger.error("create failed : NodeDevice Main is null " + " : "
					+ tagName);
			return Constant.ERROR_DISTRIBUTION_INFO;
		}
		if (distributeInfo.tmp != null) {
			try {
				Client agilor=getAgilor(distributeInfo.tmp);
				if(agilor==null){
					return Constant.ERROR_AGILORINI_FAIL;
				}
				logger.info("create : NodeDevice Tmp is created " + " : "
						+ tagName);
				return result==0?callback.tmpNodeCallBack(agilor):result;

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return Constant.ERROR_DEFAULT;
			}
		}else{
			return Constant.SUCESS;
		}
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
