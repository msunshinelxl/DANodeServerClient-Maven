package com.agilor.distribute.test;

import java.util.Random;
import java.util.TreeMap;

import com.agilor.distribute.consistenthash.ConsistentHash;
import com.agilor.distribute.consistenthash.MD5Hash;
import com.agilor.distribute.consistenthash.Node;
import com.agilor.distribute.consistenthash.NodeDevice;

public class ConsistenthashStressTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//Pre test init Node
		ConsistentHash<Node> nodeInfo=new ConsistentHash<Node>(new MD5Hash());
		for(int i=0;i<100;i++){
			nodeInfo.add(new Node("127.0.0."+String.valueOf(i), 300, "node"+String.valueOf(i), i));
		}
		long st=System.currentTimeMillis();
		Random r=new Random();
		TreeMap<String, NodeDevice>match=new TreeMap<String, NodeDevice>();
		for(int i=0;i<100000;i++){
			match.put( String.valueOf(i),nodeInfo.get(String.valueOf(i)));
		}
		for(int i=0;i<100000;i++){
			NodeDevice tmp=match.get(String.valueOf(i));
			if(tmp.getNode().getName().compareTo(nodeInfo.get(String.valueOf(i)).getNode().getName())!=0){
				System.err.println(i);
			}
		}
		System.out.println(System.currentTimeMillis()-st);
	}

}
