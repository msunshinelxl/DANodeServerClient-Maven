package com.agilor.distribute.consistenthash;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;


public class Node implements Comparable<Node> {
	 private int id;
	 private String ip;
	 private int virtualNum=-1;
	 private String name;
//	 private ArrayList<Integer> openPorts = new ArrayList<Integer>();
	 
	 private List<String> virtualNodes = new ArrayList<String>();
	 
	 private List<Integer> data = new ArrayList<Integer>();
	
//     public Node()
//     {
//    	 this(null,null,-1);
//     }
     
     
     public Node(String ip,int virtualNum,String name,int id)
     {
    	 this.ip=ip;
    	 if(virtualNum>0){
    		 this.virtualNum=virtualNum;
    	 }
    	 setId(id);
    	 setName(name);
     }
     
     public Node(Node in){
    	 this.ip=in.ip;
    	 if(in.virtualNum>0){
    		 this.virtualNum=in.virtualNum;
    	 }
    	 setId(in.id);
    	 setName(in.name);
     }
     
     public Node(JSONObject tmpJo){
    	 this.ip=tmpJo.getString("ip");
		 this.virtualNum=tmpJo.getInt("virtualNum");
    	 this.name=tmpJo.getString("name");
    	 this.id=tmpJo.getInt("id");
     }
     
     private void setName(String name){
    	 this.name=name;
     }
     public String getName(){
    	 return this.name;
     }
     public int getVirtualNum(){
    	 return this.virtualNum;
     }
     
     
     public void addData(Integer item) {
		data.add(item);
     }
     
     public Integer dataSize()
     {
    	 return data.size();
     }
     
     
     
     
     public void addVirtualNode(String node)
     {
    	 virtualNodes.add(node);
     }
     public void removeVirtualNode(String node)
     {
    	 virtualNodes.remove(node);
     }


	public int getId() {
		return id;
	}


	private void setId(int id) {
		this.id = id;
	}


	public String getIp() {
		return ip;
	}


	public void setIp(String ip) {
		this.ip = ip;
	}



	@Override
	public int compareTo(Node o) {
		// TODO Auto-generated method stub
		return name.compareTo(o.name);
	}
	public Map nodeToMap(){
		HashMap res=new HashMap();
		res.put("name", name);
		res.put("ip", ip);
		res.put("virtualNum", virtualNum);
		res.put("id", id);
		return res;
	}
     
     
}
