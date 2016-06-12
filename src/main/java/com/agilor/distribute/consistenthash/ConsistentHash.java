package com.agilor.distribute.consistenthash;

import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.agilor.distribute.common.ComFuncs;
import com.agilor.distribute.common.Interface.ConsistentHashVirtualNodeTravel;

public class ConsistentHash<T extends Node> {

	private final HashFunction hashFunction;
//	private final int numberOfReplicas;
	private final SortedMap<Integer, T> circle = new TreeMap<Integer, T>();
	private final ReentrantReadWriteLock myLock=new ReentrantReadWriteLock(false);
	private TreeSet<Node> nodeSet;
	public ConsistentHash(HashFunction hashFunction) {
		this.hashFunction = hashFunction;
		nodeSet=new TreeSet<Node>();
	}
	

	
	/**
	 * return  0:OK
     *        -1:node existed
     *        -2:unknown error
	 *
	 * */
	public int add(T node) {
		myLock.writeLock().lock();
		//add virtual nodes
		if(nodeSet.contains(node)){
			myLock.writeLock().unlock();
			return -1;
		}else{
			nodeSet.add(node);
		}
		ComFuncs.travelInConsistentHash(node,new ConsistentHashVirtualNodeTravel(){

			@Override
			public void inFor(String id) {
				// TODO Auto-generated method stub
				Integer hash = hashFunction.hash(id);
				
				node.addVirtualNode(id);
				
				circle.put(hash, node);
			}
			
		});
//		for (int i = 0; i < node.getVirtualNum(); i++) {
//			String id = createId(node, i);
//			Integer hash = hashFunction.hash(id);
//			
//			node.addVirtualNode(id);
//			
//			circle.put(hash, node);
//		}
		myLock.writeLock().unlock();
		return 0;
	}

	public void remove(T node) {
		myLock.writeLock().lock();
		ComFuncs.travelInConsistentHash(node,new ConsistentHashVirtualNodeTravel(){

			@Override
			public void inFor(String id) {
				// TODO Auto-generated method stub
				circle.remove(id);
			}
			
		});
        if(nodeSet.contains(node)){
            nodeSet.remove(node);
        }
//		for (int i = 0; i < node.getVirtualNum(); i++) {
//			circle.remove(createId(node, i));
//		}
		myLock.writeLock().unlock();
	}

	public NodeDevice get(String key) {
		myLock.readLock().lock();
		if (circle.isEmpty()) {
			myLock.readLock().unlock();
			return null;
		}
		Integer hash = hashFunction.hash(key);
		if (!circle.containsKey(hash)) {
			SortedMap<Integer, T> tailMap = circle.tailMap(hash);
			hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
		}
		NodeDevice res=new NodeDevice(circle.get(hash),String.valueOf(hash));
		myLock.readLock().unlock();
		return res;
	}

    public Node getNextNode(Node mynode){
        myLock.readLock().lock();
        if (nodeSet.isEmpty()) {
            myLock.readLock().unlock();
            return null;
        }
        SortedSet<Node> subset=nodeSet.tailSet(mynode);
        myLock.readLock().unlock();
        return subset.isEmpty()?nodeSet.first():subset.first();
    }

//	public SortedMap<Integer, T> getVirtualNodes()
//	{
//		if(circle.isEmpty()) return null;
//		//return new ArrayList<T>(circle.values());
//		return circle;
//	}
	
	public TreeSet<Node> getNodes(){
		myLock.readLock().lock();
		if(nodeSet.isEmpty()){
			myLock.readLock().unlock();
			return null;
		}
		TreeSet<Node> res = new TreeSet<Node>();
		res.addAll(nodeSet);
		myLock.readLock().unlock();
		return res;
	}

	public void clear(){
		myLock.writeLock().lock();
		nodeSet.clear();
		circle.clear();
		myLock.writeLock().unlock();
	}


    public void showContent(){
        nodeSet.forEach((Node item)->{
            System.out.println(item.nodeToMap().toString());
        });
    }
}