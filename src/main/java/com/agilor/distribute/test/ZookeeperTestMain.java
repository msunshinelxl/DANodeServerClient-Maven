package com.agilor.distribute.test;

import java.util.Iterator;
import java.util.TreeSet;

import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.*;

import com.agilor.distribute.consistenthash.Node;
public class ZookeeperTestMain {
	public static final String ip="101.200.77.14";
	public static ZooKeeper  zk=null;
	public static void main(String[] args) throws Exception
	{
		 // 创建一个与服务器的连接
//		 zk = new ZooKeeper(ip+":" + "2181", 
//		        3000, new Watcher() { 
//		            // 监控所有被触发的事件
//		            public void process(WatchedEvent event) { 
//		                System.out.println("已经触发了" + event.getType() + "事件！In Path:"+event.getPath()); 
//		                if(event.getPath()!=null&&zk!=null){
//		                	try {
//								zk.exists(event.getPath(), true, new StatCallback(){
//
//									@Override
//									public void processResult(int rc,
//											String path, Object ctx, Stat stat) {
//										// TODO Auto-generated method stub
//										 System.out.println("已经触发了" + stat.getNumChildren() + "事件！In Path:"+event.getPath());
//										 try {
//											zk.getData("/testRootPath",true,null);
//										} catch (KeeperException e) {
//											// TODO Auto-generated catch block
//											e.printStackTrace();
//										} catch (InterruptedException e) {
//											// TODO Auto-generated catch block
//											e.printStackTrace();
//										}
//									}
//									
//								}, null);
//							} catch (Exception e) {
//								// TODO Auto-generated catch block
//								e.printStackTrace();
//							} 
//		                }
//		            } 
//		        }); 
//		 // 创建一个目录节点
//		 zk.create("/testRootPath", "testRootData".getBytes(), Ids.OPEN_ACL_UNSAFE,
//		   CreateMode.PERSISTENT);
//		 System.out.println(new String(zk.getData("/testRootPath",true,null))); 
//		 // 创建一个子目录节点
//		 zk.create("/testRootPath/testChildPathOne", "testChildDataOne".getBytes(),
//		   Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT); 
//
//		 // 取出子目录节点列表
//		 //System.out.println(zk.getChildren("/testRootPath",true)); 
//		 // 修改子目录节点数据
//		 zk.setData("/testRootPath","modifyChildDataOne".getBytes(),-1); 
//		 //System.out.println("目录节点状态：["+zk.exists("/testRootPath",true)+"]"); 
//		 // 创建另外一个子目录节点
//		 zk.create("/testRootPath/testChildPathTwo", "testChildDataTwo".getBytes(), 
//		   Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT); 
//		 //System.out.println(new String(zk.getData("/testRootPath/testChildPathTwo",true,null))); 
//		 // 删除子目录节点
//		 zk.delete("/testRootPath/testChildPathTwo",-1); 
//		 zk.delete("/testRootPath/testChildPathOne",-1); 
//		 // 删除父目录节点
//		 zk.delete("/testRootPath",-1); 
//		 // 关闭连接
//		 zk.close();
		 TreeSet<Node> set = new TreeSet<Node>();
		 set.add(new Node("0.0.0.0", 300, "node1", 1));
	        set.add(new Node("0.0.0.0",  300, "node2", 1));
	        set.add(new Node("0.0.0.0", 300, "node3", 1));
	        set.add(new Node("0.0.0.0",  300, "node4", 1));
	        set.add(new Node("0.0.0.0",  300, "node5", 1));
	        set.add(new Node("0.0.0.0",  300, "node6", 1));
	        if(set.contains(new Node("0.0.0.0",  300, "node2", 1))==true){
	        	System.out.println("hah");
	        for(Iterator<Node> iter = set.iterator(); iter.hasNext(); ) {
	            System.out.printf("asc : %s\n", iter.next().getName());
	        }
	        }


	}
}
