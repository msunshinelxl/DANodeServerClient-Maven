package com.agilor.distribute.test;

import com.agilor.distribute.common.Constant;
import com.agilor.distribute.consistenthash.MD5Hash;
import com.agilor.distribute.consistenthash.Node;
import com.agilor.distribute.server.nameManager.NodeHandler;
import org.ini4j.Ini;
import org.ini4j.InvalidFileFormatException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ServerTestMain {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Ini ini = new Ini();
		String hostPort = Constant.ZK_IP+":"+Constant.ZK_PORT;
		String myNodeIP="0.0.0.0";
		int VNodeNum=300;
		String nodeName="node1";
		int nodeID=1;
		Path currentRelativePath = Paths.get("");
		String s = currentRelativePath.toAbsolutePath().toString();
        try {
			ini.load(new FileReader(s+"/profile.ini"));
			hostPort=ini.get("Zookeeper").get("HostPort");
			myNodeIP=ini.get("MyNode").get("IP");
			VNodeNum=Integer.valueOf(ini.get("MyNode").get("VirtualNodeNumber"));
			nodeID=Integer.valueOf(ini.get("MyNode").get("Id"));
			nodeName=ini.get("MyNode").get("Name");
		} catch (InvalidFileFormatException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
//		String hostPort = "101.200.77.14:2181";
		Node myNode = new Node(myNodeIP, VNodeNum, nodeName, nodeID);
		MD5Hash tmpHash=new MD5Hash();
//		try {
//			Agilor aClient=new Agilor(myNodeIP,9090,20000);
//			aClient.open();
		
//			ComFuncs.travelInConsistentHash(myNode, new ConsistentHashVirtualNodeTravel() {
//
//				@Override
//				public void inFor(String vName) {//					// TODO Auto-generated method stub
//
//					Device device=new Device();
//					device.setName(String.valueOf(tmpHash.hash(vName)));
//					try {
//						aClient.insert(device);
//					} catch (TException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				}
//			});
//			aClient.close();
//		}catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		System.out.println("init fine");
		//zookeeper 
		NodeHandler test1=new NodeHandler(hostPort,"/NodeInfo","/ClientNodeInfo",myNode);
		test1.stratHandler();
		
	}

}
