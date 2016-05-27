package com.agilor.distribute.server.nameManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import agilor.distributed.communication.client.Client;
import agilor.distributed.communication.client.Value;
import agilor.distributed.communication.protocol.SimpleProtocol;
import agilor.distributed.communication.result.GetAllTagResultFuture;
import agilor.distributed.communication.result.GetValueResultFuture;
import agilor.distributed.communication.result.TagInfoRes;
import agilor.distributed.communication.result.ValueRes;
import agilor.distributed.communication.socket.Connection;
import com.agilor.distribute.consistenthash.NodeDevice;
import com.agilor.distribute.test.LogTestMain;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


import com.agilor.distribute.common.ComFuncs;
import com.agilor.distribute.common.Constant;
import com.agilor.distribute.common.Interface.ZnodeGetDataCallback;
import com.agilor.distribute.consistenthash.ConsistentHash;
import com.agilor.distribute.consistenthash.MD5Hash;
import com.agilor.distribute.consistenthash.Node;
import com.agilor.distribute.zookeeper.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeHandler {
	String znodeServerInfo;
	String znodeClientInfo;
	String hostPort;
	Executor taskExecutor;
	ZnodeGetDataCallback taskCallback;
	Node myNode;
	ConsistentHash nodeList;
	int finishTrsCounter;
	final static Logger logger = LoggerFactory.getLogger(LogTestMain.class);
	public NodeHandler(String hostPort, String znodeServerInfo,
			String znodeClientInfo, Node myNode) {
		this.hostPort = hostPort;
		this.znodeServerInfo = znodeServerInfo;
		this.znodeClientInfo = znodeClientInfo;
		this.myNode = myNode;
		taskCallback = null;
		nodeList = new ConsistentHash(new MD5Hash());
		try {
			taskExecutor = new Executor(hostPort, Constant.zkRootPath
					+ znodeServerInfo,Constant.zkTimeLong);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			System.err.println("taskExecutor in NodeHandler error");
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.err.println("taskExecutor in NodeHandler error");
			e.printStackTrace();
		}
		finishTrsCounter=0;
	}

	public void stratHandler() {
		taskCallback = new ZnodeGetDataCallback() {

			@Override
			public void todo(Executor parent, byte[] data) {
				// TODO Auto-generated method stub
				String str_tmp = new String(data);
				try {
					JSONObject receiveJo = new JSONObject(str_tmp);
					JSONArray receiveJa = receiveJo.getJSONArray("content");
					TreeSet<Node> tmpTreeSet = new TreeSet<Node>();

					for (int i = 0; i < receiveJa.length(); i++) {
						JSONObject tmpJo = receiveJa.getJSONObject(i);
						tmpTreeSet
								.add(new Node(tmpJo.getString("ip"),
										tmpJo.getInt("virtualNum"), tmpJo
												.getString("name"), tmpJo
												.getInt("id")));
					}
					Iterator<Node> iterator1 = tmpTreeSet.iterator();
					if(nodeList.getNodes()==null){
						//first init
						System.out.println("first init");
						while (iterator1.hasNext()) {
							Node tmpItem=iterator1.next();
							nodeList.add(tmpItem);
//							System.out.println(tmpItem);
						}
					}
					if (tmpTreeSet.contains(myNode)) {
						// I'm not new node
						System.out.println("I'm not new node");
						for (Iterator<Node> iter1 = tmpTreeSet.iterator(); iter1
								.hasNext();) {
//							consistentHashLock.readLock().lock();
							Node tmpNode = iter1.next();
							if (!nodeList.getNodes().contains(tmpNode)) {
								// transfer data to new node
								System.out.println("find New Node : "+tmpNode.getName());
								nodeList.add(tmpNode);
								try {
									parent.zk.create(
											Constant.zkRootPath
													+ znodeServerInfo + "/"
													+ tmpNode.getName() + "/"
													+ myNode.getName(),
											"start transfer".getBytes(),
											Ids.OPEN_ACL_UNSAFE,
											CreateMode.PERSISTENT);
									//migrate my tag to new Node
									List<TagInfoRes>tmpRes=addTag2NewNode(tmpNode);
									for(int i=0;i<tmpRes.size();i++){
										System.out.println(tmpRes.get(i).tagName);
									}
									//write
                                    writeHisVal2NewNode(tmpRes,tmpNode);
                                    //end write
									parent.zk.delete(
											Constant.zkRootPath
													+ znodeServerInfo + "/"
													+ tmpNode.getName() + "/"
													+ myNode.getName(), -1);
									System.out.println("delete :"
											+ Constant.zkRootPath
											+ znodeServerInfo + "/"
											+ tmpNode.getName() + "/"
											+ myNode.getName());
								} catch (KeeperException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
//							consistentHashLock.readLock().unlock();
						}

					} else {
						try {
							// I'm new node
							System.out.println("I'm new node or delete node");
							// create client
							Stat client = parent.zk.exists(Constant.zkRootPath
									+ znodeClientInfo, false);
							if (client == null) {
								//if clientInfo in zk is null init value
								JSONObject tmpJo = new JSONObject();
								JSONArray tmpJa = new JSONArray();
								tmpJa.put(myNode.nodeToMap());
								tmpJo.put(Constant.zkNodeClientFinalListName,
										tmpJa);
								parent.zk.create(Constant.zkRootPath
										+ znodeClientInfo, tmpJo.toString()
										.getBytes(), Ids.OPEN_ACL_UNSAFE,
										CreateMode.PERSISTENT);
							} else {
								//put my info to Client
								JSONObject clientJO = ComFuncs
										.byte2Json(parent.zk.getData(
												Constant.zkRootPath
														+ znodeClientInfo,
												null, client));
								JSONArray tmpJa = null;
								//put TMP info to zk Client
								if (!clientJO
										.has(Constant.zkNodeClientTmpListName) ) {
									tmpJa = new JSONArray();
								} else {
									tmpJa = clientJO
											.getJSONArray(Constant.zkNodeClientTmpListName);
								}
								tmpJa.put(myNode.nodeToMap());
								clientJO.put(Constant.zkNodeClientTmpListName,
										tmpJa);
								//put TMP info to zk Client
								if (clientJO
										.has(Constant.zkNodeClientFinalListName)) {
									tmpJa = clientJO
											.getJSONArray(Constant.zkNodeClientFinalListName);
									tmpJa.put(myNode.nodeToMap());
									clientJO.put(
											Constant.zkNodeClientFinalListName,
											tmpJa);
								} else {
									System.out.println("Client Info in Zk error! when New one comes");
								}
								parent.zk.setData(Constant.zkRootPath
										+ znodeClientInfo, clientJO.toString()
										.getBytes(), -1);
							}
							parent.zk.create(Constant.zkRootPath
									+ znodeServerInfo + "/" + myNode.getName(),
									"addNode".getBytes(), Ids.OPEN_ACL_UNSAFE,
									CreateMode.PERSISTENT);
							Iterator<Node> iter1 = tmpTreeSet.iterator();
							ReentrantReadWriteLock finishTrsCounterLock = new ReentrantReadWriteLock(
									false);
							while (iter1.hasNext()) {
								Node tmpNode = iter1.next();
								if (tmpNode.getName().compareTo(
										myNode.getName()) != 0) {
									new Executor(hostPort, Constant.zkRootPath
											+ znodeServerInfo + "/"
											+ myNode.getName() + "/"
											+ tmpNode.getName(),Constant.zkTimeNormal,
											new ZnodeGetDataCallback() {

												@Override
												public void todo(Executor mine,
														byte[] data) {
													// TODO
													// Auto-generated
													// method stub
													System.out
															.println("subReceive "+new String(
																	data));
												}

												@Override
												public void onClose(
														Executor parent) {
													// TODO Auto-generated
													// method stub
													finishTrsCounterLock
															.writeLock().lock();
													finishTrsCounter++;
													if (finishTrsCounter == tmpTreeSet
															.size()) {
														System.out
																.println("finish Transfer");
														try {
															parent.zk
																	.delete(Constant.zkRootPath
																			+ znodeServerInfo
																			+ "/"
																			+ myNode.getName(),
																			-1);
														} catch (InterruptedException e) {
															// TODO
															// Auto-generated
															// catch block
															e.printStackTrace();
														} catch (KeeperException e) {
															// TODO
															// Auto-generated
															// catch block
															e.printStackTrace();
														}
														try {
															JSONObject clientJO = ComFuncs
																	.byte2Json(parent.zk
																			.getData(
																					Constant.zkRootPath
																							+ znodeClientInfo,
																					null,
																					client));
															JSONArray tmpJa = null;
															if (clientJO
																	.has(Constant.zkNodeClientTmpListName)) {
																tmpJa = clientJO
																		.getJSONArray(Constant.zkNodeClientTmpListName);
																for (int i = 0; i < tmpJa
																		.length(); i++) {
																	JSONObject tmpJo = (JSONObject) tmpJa
																			.get(i);
																	if (tmpJo
																			.has("name")&& (tmpJo
																					.getString("name"))
																					.compareTo(myNode.getName()) == 0) {
																		tmpJa.remove(i);
																	}
																}
																clientJO.put(
																		Constant.zkNodeClientTmpListName,
																		tmpJa);
																parent.zk
																		.setData(
																				Constant.zkRootPath
																						+ znodeClientInfo,
																				clientJO.toString()
																						.getBytes(),
																				-1);
																System.out
																		.println(Constant.zkNodeClientTmpListName
																				+ " modified");
															} else {
																System.out
																		.println(Constant.zkNodeClientTmpListName
																				+ " dont exist");
															}
														} catch (KeeperException e) {
															// TODO
															// Auto-generated
															// catch block
															e.printStackTrace();
														} catch (InterruptedException e) {
															// TODO
															// Auto-generated
															// catch block
															e.printStackTrace();
														}finally {
															finishTrsCounter=0;
														}

													}
													finishTrsCounterLock
															.writeLock()
															.unlock();
													try {
														parent.zk.close();
													} catch (InterruptedException e) {
														e.printStackTrace();
													}
												}
											}).start();
								}
							}
//							consistentHashLock.writeLock().lock();
							nodeList.add(myNode);
							receiveJa.put(myNode.nodeToMap());
							JSONObject jo_tmp = new JSONObject(str_tmp);
							jo_tmp.put("content", receiveJa);
							parent.setData(receiveJo.toString());
//							consistentHashLock.writeLock().unlock();
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} finally {
							if (parent.zk != null) {
								if (!nodeList.getNodes().isEmpty()
										&& nodeList.getNodes().size() == 1) {
									try {
										parent.zk.delete(Constant.zkRootPath
												+ znodeServerInfo + "/"
												+ myNode.getName(), -1);
									} catch (InterruptedException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									} catch (KeeperException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
							}
						}
					}
				} catch (JSONException e) {
					System.out.println(e.toString());

				}
			}

			@Override
			public void onClose(Executor p) {
				// TODO Auto-generated method stub
			}
		};
		taskExecutor.setGetdataCallback(taskCallback);
		taskExecutor.start();
	}

	// public void startExecutor() {
	// taskExecutor.start();
	// }
	public ConsistentHash getNodeList(){
		if(nodeList==null){
			nodeList=new ConsistentHash(new MD5Hash());
		}
		return nodeList;
	}
	private List<TagInfoRes> addTag2NewNode(Node newNode){
		Client agilor,newAgilor;
		List<TagInfoRes> migrateTargets=null;
		try {
			agilor = new Client(new Connection(myNode.getIp(), 10001, SimpleProtocol.getInstance()));

			newAgilor=new Client(new Connection(newNode.getIp(), 10001, SimpleProtocol.getInstance()));//new Agilor(newNode.getIp(),Constant.agilorNodeThriftPort,Constant.agilorNodeThriftLongTimeout);
			newAgilor.open();
			agilor.open();
			GetAllTagResultFuture allTagRes=(GetAllTagResultFuture)agilor.getAllTag();
            List<TagInfoRes>tList=allTagRes.get(-1,null);
			migrateTargets=new ArrayList<>();

			ConsistentHash tmpNodeList=getNodeList();
			for(int j=0;j<tList.size();j++){
				NodeDevice tmpNodeDevice=tmpNodeList.get(tList.get(j).tagName);
				if(newNode.getIp().compareTo(tmpNodeDevice.getNode().getIp())==0&&!tList.get(j).tagName.contains(Constant.deviceNamePre)){
					migrateTargets.add(tList.get(j));
					ComFuncs.createTag(newAgilor, tList.get(j).tagName, tmpNodeDevice.getDevice(), logger,new Value(tList.get(j).type));
//						System.out.println("Created : "+tList.get(j).getName());
				}
			}
			newAgilor.close();
			agilor.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			return migrateTargets;
		}
	}

	private void writeHisVal2NewNode(List<TagInfoRes> tags,Node newNode){
		ConsistentHash tmpNodeList=getNodeList();
		if(tmpNodeList==null){
			logger.error("nodelist is null");
			return ;
		}
        Calendar start = Calendar.getInstance();
        Calendar end = Calendar.getInstance(TimeZone.getTimeZone("GMT"),Locale.CHINA);

        start.set(Calendar.YEAR,1980);
        start.set(Calendar.MONTH,1);
        start.set(Calendar.DAY_OF_MONTH,1);
        start.set(Calendar.HOUR_OF_DAY,1);
        start.set(Calendar.MINUTE, 1);
        start.set(Calendar.SECOND,1);
		try {
			logger.info("enter writeing setion new IP:"+newNode.getIp()+" myNodeIP:"+myNode.getIp());
			Client newAgilor = new Client(new Connection(newNode.getIp(), 10001, SimpleProtocol.getInstance()));//new Agilor(newNode.getIp(), Constant.agilorNodeThriftPort, Constant.agilorNodeThriftTimeout);
			Client myAgilor=new Client(new Connection(myNode.getIp(), 10001, SimpleProtocol.getInstance()));
			logger.info("start open");
			newAgilor.open();
			myAgilor.open();
			for(int i=0;i<tags.size();i++){
				NodeDevice tmpNodeDevice = tmpNodeList.get(tags.get(i).tagName);
				if(tmpNodeDevice.getNode().getIp().compareTo(newNode.getIp())!=0){
					logger.error(tags.get(i).tagName+" : migrate write error not belong to "+newNode.getIp());
					continue;
				}
                GetValueResultFuture resVal= (GetValueResultFuture)myAgilor.getValue(start, end, tags.get(i).tagName);
                List<ValueRes> values=resVal.get(-1,null);
				logger.info(tags.get(i).tagName+" : sizeof "+String.valueOf(values.size()));
                for(int j=0;j<values.size();j++){
                    ComFuncs.writeTagValue(newAgilor,tags.get(i).tagName,values.get(j).value,tmpNodeDevice.getDevice(),false);
                }
			}
			myAgilor.close();
			newAgilor.close();
		}catch (Exception e){
			logger.error(e.toString());
		}


	}
}
