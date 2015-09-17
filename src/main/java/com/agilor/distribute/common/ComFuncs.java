package com.agilor.distribute.common;
import java.util.ArrayList;
import java.util.List;

import agilor.distributed.storage.inter.jlient.Agilor;
import agilor.distributed.storage.inter.jlient.Device;
import agilor.distributed.storage.inter.jlient.Target;
import agilor.distributed.storage.inter.jlient.Val;
import com.agilor.distribute.consistenthash.NodeDevice;
import org.json.JSONObject;

import com.agilor.distribute.common.Interface.ConsistentHashVirtualNodeTravel;
import com.agilor.distribute.consistenthash.Node;
import org.slf4j.Logger;

public class ComFuncs {
	public static JSONObject byte2Json(byte[] inputData){
		String tmpString=new String(inputData);
		JSONObject res=new JSONObject(tmpString);
		return res;
	}
	
	public static List<String> getAll(Node node,int index)
	{
		List<String>res=new ArrayList<String>();
		travelInConsistentHash(node,new ConsistentHashVirtualNodeTravel(){

			@Override
			public void inFor(String vName) {
				// TODO Auto-generated method stub
				res.add(vName);
			}
			
		});
		return res;
	}
	
	public static void travelInConsistentHash(Node node,ConsistentHashVirtualNodeTravel indoing){
		for (int i = 0; i < node.getVirtualNum(); i++) {
			String id = node.getIp()+"#"+i;
			indoing.inFor(id);

		}
	}

	public static boolean createTag(Agilor singleClient,String tagName, Device device,Logger logger)
			throws Exception {
		if (device== null) {
			logger.error("create failed : Node is null " + " : " + tagName);
			return false;
		}
		singleClient.attach(device);
		Target target = new Target();
		target.setName(tagName);
		target.setDeviceName(device.getName());
		target.setGroupName(tagName);
		boolean res = device.insert(target);
		return res;
	}

	public static void writeTagValue(Agilor singleClient,String tagName, Val value,
							   Device device,Logger logger) throws Exception {
		if (device == null) {
			logger.error("write failed : Node is null " + " : " + tagName);
			return;
		}
		singleClient.attach(device);
		Target target = new Target();
		singleClient.attach(target);
		target.setName(tagName);
		target.setDeviceName(device.getName());
		target.setGroupName(tagName);
		target.write(value);
	}

	public static void writeTagValue(Agilor singleClient,Target target, Val value,
									 Device device,Logger logger) throws Exception {
		if (device == null) {
			logger.error("write failed : Node is null " + " : " + target.getName());
			return;
		}
		if(target==null){
			logger.error("write failed : tag is null " + " : " + target.getName());
			return;
		}
		singleClient.attach(device);
		singleClient.attach(target);
		target.setDeviceName(device.getName());
		target.write(value);
	}
}
