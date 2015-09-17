package com.agilor.distribute.consistenthash;

import agilor.distributed.storage.inter.jlient.Device;

public class NodeDevice {
	private Node myNode;
	private Device myDevice;
	public NodeDevice(Node node,String deviceName){
		myNode=new Node(node);
		myDevice=new Device();
		myDevice.setName(deviceName);
	}
	public Node getNode(){
		return myNode;
	}
	public Device getDevice(){
		return myDevice;
	}
}
