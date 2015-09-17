package com.agilor.distribute.common;

import java.util.TreeSet;

import com.agilor.distribute.consistenthash.Node;
import com.agilor.distribute.zookeeper.Executor;

public interface Interface {
	public interface ZnodeGetDataCallback {
		public void todo(Executor mine, byte[] data);
		public void onClose(Executor parent);
	}
	
//	public interface NodeManagerCallback{
//		public void onMigration(Executor parent,TreeSet<Node> nodeSet );
//		public void onJoinCluster();
//
//	}
	
	public interface ConsistentHashVirtualNodeTravel{
		public void inFor(String vName);
	}
}
