package com.agilor.distribute.common;

import agilor.distributed.communication.client.Client;
import agilor.distributed.communication.client.Value;
import com.agilor.distribute.common.Interface.ConsistentHashVirtualNodeTravel;
import com.agilor.distribute.consistenthash.Node;
import org.json.JSONObject;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

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

	public static boolean createTag(Client singleClient,String tagName, String device,Logger logger,Value val)
			throws Exception {
		if (device== null) {
			logger.error("create failed : Node is null " + " : " + tagName);
			return false;
		}
//		singleClient.open();
//		Value val = new Value(Value.Types.FLOAT);
//        setVal(valval, val);
		singleClient.addTarget(tagName,device, val);
//		singleClient.close();
		return true;
	}

    public static boolean setVal(Object valval,Value val){
        if(valval.getClass()==Float.class){
            val.setFvalue((Float)valval);
        }else if(valval.getClass()==Integer.class){
            val.setLvalue((Integer) valval);
        }else if(valval.getClass()==String.class){
            val.setSvalue((String) valval);
        }else if(valval.getClass()==Boolean.class){
            val.setBvalue((Boolean) valval);
        }else{
            return false;
        }

        return true;
    }

	public static void writeTagValue(Client singleClient,String tagName, Value value,Logger logger,String Device) throws Exception {
//		if (device == null) {
//			logger.error("write failed : Node is null " + " : " + tagName);
//			return;
//		}
//		singleClient.attach(device);
//		Target target = new Target();
//		singleClient.attach(target);
//		target.setName(tagName);
//		target.setDeviceName(device.getName());
//		target.setGroupName(tagName);
//		target.write(value);
        singleClient.addValue(tagName, Device, value);
	}

//	public static void writeTagValue(Agilor singleClient,Target target, Val value,
//									 Device device,Logger logger) throws Exception {
//		if (device == null) {
//			logger.error("write failed : Node is null " + " : " + target.getName());
//			return;
//		}
//		if(target==null){
//			logger.error("write failed : tag is null " + " : " + target.getName());
//			return;
//		}
//		singleClient.attach(device);
//		singleClient.attach(target);
//		target.setDeviceName(device.getName());
//		target.write(value);
//	}
}
