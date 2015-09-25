package com.agilor.distribute.zookeeper;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.agilor.distribute.common.Interface;
import com.agilor.distribute.common.Interface.ZnodeGetDataCallback;
import com.agilor.distribute.common.NodeStateType;

public class Executor extends Thread implements Watcher,
		DataMonitor.DataMonitorListener {

	String znode;

	DataMonitor dm;

	public ZooKeeper zk;

	ZnodeGetDataCallback getdataCallback;
	
	
	private AtomicReference<NodeStateType> stat;
	
	public Executor(String hostPort, String znode,int timeOut,
			ZnodeGetDataCallback getdataCallback) throws KeeperException,
			IOException {
		init(hostPort,znode,timeOut);
		this.getdataCallback = getdataCallback;
		
	}
	
	public Executor(String hostPort, String znode,int timeOut) throws KeeperException,
	IOException {
		init(hostPort,znode,timeOut);
		this.getdataCallback = null;
	}



	private void init(String hostPort, String znode,int timeOut)throws KeeperException,
	IOException{
		this.znode = znode;
		zk = new ZooKeeper(hostPort, timeOut, this);
		dm = new DataMonitor(zk, znode, null, this);
		stat=new AtomicReference<NodeStateType>();
	}


	public void run() {
		try {
			synchronized (this) {
				stat.set(NodeStateType.IS_ALAVE);
				while (!dm.dead) {
					wait();
				}
				System.out.println(znode + "  died");
				if(getdataCallback!=null)
					getdataCallback.onClose(this);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void setGetdataCallback(ZnodeGetDataCallback callback){
		getdataCallback=callback;
	}
	
	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		dm.process(event);
	}

	@Override
	public void exists(byte[] data) {
		// TODO Auto-generated method stub
		if (data == null) {
			String str_tmp = new String(data);
			System.out.print(str_tmp);

		} else {
			if(getdataCallback!=null)
				getdataCallback.todo(this, data);
		}
	}

	@Override
	public void closing(int rc) {
		// TODO Auto-generated method stub
		synchronized (this) {
			notifyAll();
		}
	}

	public void setData(String data) {
		try {
			zk.setData(znode, data.getBytes(), -1);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void createTmpDict() {
		try {
			zk.create(znode, "{}".getBytes(), Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
