package com.agilor.distribute.client.nameManage;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.agilor.distribute.common.ComFuncs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import agilor.distributed.storage.inter.jlient.Agilor;
import agilor.distributed.storage.inter.jlient.Device;
import agilor.distributed.storage.inter.jlient.Target;
import agilor.distributed.storage.inter.jlient.Val;

import com.agilor.distribute.common.Constant;
import com.agilor.distribute.consistenthash.NodeDevice;
import com.agilor.distribute.test.LogTestMain;

public class AgilorDistributeClient {


	private class DistributeInfo {
		NodeDevice main = null;
		NodeDevice tmp = null;
	}
	
	private interface DistributeLogInterface{
		void mainNodeCallBack(Agilor agilor) throws Exception;
		void tmpNodeCallBack(Agilor agilor) throws Exception;
	}
	
	
	final static Logger logger = LoggerFactory.getLogger(LogTestMain.class);
	Map<String,Agilor>activityAgilor;
	public AgilorDistributeClient() {
		activityAgilor=new HashMap<String, Agilor>();
	}

//	public Agilor openSession
	public void createTagNode(String tagName) {
		DistributeInfo distributeInfo = getDistributeInfo(tagName);
		try {
			distributeLogFrame(tagName,distributeInfo,new DistributeLogInterface(){

				@Override
				public void mainNodeCallBack(Agilor agilor) throws Exception {
					// TODO Auto-generated method stub
						if (ComFuncs.createTag(agilor, tagName, distributeInfo.main.getDevice(), logger) == false)
							logger.error("create failed :"
									+ distributeInfo.main.getNode().getIp() + " : "
									+ distributeInfo.main.getDevice().getName() + " : "
									+ tagName);
				}

				@Override
				public void tmpNodeCallBack(Agilor agilor) throws Exception{
					// TODO Auto-generated method stub
					if (ComFuncs.createTag(agilor, tagName, distributeInfo.tmp.getDevice(),logger)==false)
						logger.error("create failed :"
								+ distributeInfo.tmp.getNode().getIp() + " : "
								+ distributeInfo.tmp.getDevice().getName() + " : "
								+ tagName);
				}
				
			});
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void write(String tagName,Val value){
		DistributeInfo distributeInfo=getDistributeInfo(tagName);
		try {
			
			distributeLogFrame(tagName,distributeInfo,new DistributeLogInterface(){

				@Override
				public void mainNodeCallBack(Agilor agilor) throws Exception{
					// TODO Auto-generated method stub
					ComFuncs.writeTagValue(agilor, tagName, value, distributeInfo.main.getDevice(),logger);
				}

				@Override
				public void tmpNodeCallBack(Agilor agilor) throws Exception{
					// TODO Auto-generated method stub
					ComFuncs.writeTagValue(agilor, tagName, value, distributeInfo.tmp.getDevice(),logger);
				}
			});
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void close(){
		Iterator<Entry<String, Agilor>> it = activityAgilor.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<String,Agilor> pair = it.next();
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
	private Agilor getAgilor(NodeDevice disInfo){
		String keyName=disInfo.getNode().getIp();
		if(activityAgilor.containsKey(keyName)){
			return activityAgilor.get(keyName);
		}else{
			try {
				Agilor tmpAgilor=new Agilor(disInfo.getNode().getIp(),
						Constant.agilorNodeThriftPort, Constant.agilorNodeThriftTimeout);
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
	
	
	private void distributeLogFrame(String tagName,DistributeInfo distributeInfo,DistributeLogInterface callback){
		if (distributeInfo.main != null) {
			try {
				Agilor agilor=getAgilor(distributeInfo.main);
				if(agilor==null){
					return;
				}
				callback.mainNodeCallBack(agilor);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			logger.error("create failed : NodeDevice Main is null " + " : "
					+ tagName);
		}
		if (distributeInfo.tmp != null) {
			try {
				Agilor agilor=getAgilor(distributeInfo.tmp);
				if(agilor==null){
					return;
				}
				callback.tmpNodeCallBack(agilor);
				logger.info("create : NodeDevice Tmp is created " + " : "
						+ tagName);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else{
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
