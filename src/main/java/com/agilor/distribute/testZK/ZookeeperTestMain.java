package com.agilor.distribute.testZK;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.agilor.distribute.common.Constant;
import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ZookeeperTestMain {
	public static final String connectStr= Constant.ZK_IP+":"+Constant.ZK_PORT;
	private static CountDownLatch tCount=new CountDownLatch(1);
    private Stat dataStat=new Stat();

    public void createNode(String pathStr)
            throws IOException,InterruptedException,KeeperException{
        ZooKeeper zk=new ZooKeeper(connectStr, 5000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if(Event.KeeperState.SyncConnected==watchedEvent.getState()){
                    tCount.countDown();
                }
            }
        });
        String path1=zk.create(pathStr,"".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
        System.out.println("Success! "+path1);
    }

    ZooKeeper zk;
    public void getChildren(String pathStr)
            throws IOException,InterruptedException,KeeperException{
        zk=new ZooKeeper(connectStr, 5000, (WatchedEvent watchedEvent) ->{
                if(Watcher.Event.KeeperState.SyncConnected==watchedEvent.getState()){
                    if(Watcher.Event.EventType.None==watchedEvent.getType()&&null==watchedEvent.getPath()){
                        tCount.countDown();
                    }else if(watchedEvent.getType()==Watcher.Event.EventType.NodeChildrenChanged){

                        try{
                            System.out.println(zk.getChildren(watchedEvent.getPath(),true));
                        }catch(Exception e){
                        }
                    }
                }
        });
        tCount.await();
        String path1=zk.create(pathStr,"".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        zk.create(pathStr+"/c1","".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
        //System.out.println(zk.getChildren(pathStr, true));

        zk.create(pathStr + "/c2", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zk.create(pathStr+"/c3","".getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
    }

    public void getData(String pathStr) throws IOException,InterruptedException,KeeperException{
        zk=new ZooKeeper(connectStr, 5000, (WatchedEvent watchedEvent) ->{
            if(Watcher.Event.KeeperState.SyncConnected==watchedEvent.getState()){
                if(Watcher.Event.EventType.None==watchedEvent.getType()&&null==watchedEvent.getPath()){
                    tCount.countDown();
                }else if(watchedEvent.getType()==Watcher.Event.EventType.NodeChildrenChanged){
                    try{
                        System.out.println(zk.getChildren(watchedEvent.getPath(),true));
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }else if(watchedEvent.getType()==Watcher.Event.EventType.NodeDataChanged){
                    try {
                        System.out.println(new String(zk.getData(pathStr, true, dataStat)));
                        System.out.println(dataStat.getVersion());
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }
            }
        });
        tCount.await();
        zk.create(pathStr, "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println(new String(zk.getData(pathStr, true, dataStat)));
        System.out.println(dataStat.getVersion());
        zk.setData(pathStr, "123".getBytes(), -1);
    }
    public void exists(String pathStr)throws IOException,InterruptedException,KeeperException{
        zk=new ZooKeeper(connectStr,5000,(WatchedEvent event)->{
            if(event.getState()==Watcher.Event.KeeperState.SyncConnected){
                if(event.getPath()==null&&event.getType()==Watcher.Event.EventType.None){
                    tCount.countDown();
                }else if(event.getType()==Watcher.Event.EventType.NodeChildrenChanged){
                    try{
                        System.out.println("lal");
                        System.out.println(zk.getChildren(event.getPath(),true));
                        zk.exists(pathStr, true);
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }else if(event.getType()== Watcher.Event.EventType.NodeDataChanged){
                    try{
                        System.out.println(new String(zk.getData(event.getPath(), true, null)));
                        zk.exists(pathStr, true);
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }else if(event.getType()== Watcher.Event.EventType.NodeCreated){
                    try{
                        System.out.println("NodeCreated");
                        zk.exists(event.getPath(),true);
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }else if(event.getType()== Watcher.Event.EventType.NodeDeleted){
                    try{
                        System.out.println("NodeDeleted");
                        zk.exists(event.getPath(),true);
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }
            }else{
                System.err.println("error connect!");
            }
        });
        tCount.await();
        Stat tmpStat=zk.exists(pathStr, true);
        if(tmpStat!=null){
            System.out.println(tmpStat.getVersion());
        }
        zk.create(pathStr, "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        zk.setData(pathStr, "123".getBytes(), -1);
//        System.out.println(zk.getChildren(pathStr,true
//                (WatchedEvent event)->{
//            if(event.getType()==Watcher.Event.EventType.NodeChildrenChanged) {
//                try {
//                    System.err.println("hah");
//                    System.out.println(zk.getChildren(event.getPath(), true));
//                    zk.exists(pathStr, true);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        ));
        zk.create(pathStr + "/c1", "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        zk.delete(pathStr + "/c1", -1);

        zk.delete(pathStr,-1);
    }
	public static void main(String[] args) throws Exception
	{
		 // 创建一个与服务器的连接
        ZookeeperTestMain test = new ZookeeperTestMain();
        test.exists("/zk-test-ep");
        System.out.println("hah");
	}
}
