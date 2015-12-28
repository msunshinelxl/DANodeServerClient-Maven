package com.agilor.distribute.consistenthash;



public class NodeDevice {
	private Node myNode;
	private String myDevice;
	public NodeDevice(Node node,String deviceName){
		myNode=new Node(node);
		myDevice=new String();
		myDevice=(deviceName);
	}
	public Node getNode(){
		return myNode;
	}
	public String getDevice(){
		return myDevice;
	}
}
