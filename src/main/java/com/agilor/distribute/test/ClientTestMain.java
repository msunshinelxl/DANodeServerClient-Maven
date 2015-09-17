package com.agilor.distribute.test;

import agilor.distributed.storage.inter.jlient.Agilor;
import agilor.distributed.storage.inter.jlient.Device;
import agilor.distributed.storage.inter.jlient.Target;
import agilor.distributed.storage.inter.jlient.Val;

import com.agilor.distribute.client.nameManage.AgilorDistributeClient;



public class ClientTestMain {
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		String hostPort = "101.200.77.14:2181";
//		ClientNodeHandler test1=new ClientNodeHandler(hostPort,"/ClientNodeInfo");
//		test1.stratHandler();
		AgilorDistributeClient test=new AgilorDistributeClient();
//		for(int i=0;i<10000;i++)
//			test.createTagNode("test1"+String.valueOf(i));
		long st=System.currentTimeMillis();
		for(int i=0;i<10000;i++)
		{
			Val value=new Val((float) 12.3);
			test.write("test1", value);
		}
		System.out.println("方法1(1w):耗时:"+String.valueOf((System.currentTimeMillis()-st)));
//		st=System.currentTimeMillis();
//		for(int i=0;i<100000;i++)
//		{
//			Val value=new Val((float) 12.3);
//			test.write("test1", value);
//		}
//		System.out.println("方法1(10w):耗时:"+String.valueOf((System.currentTimeMillis()-st)));
//		st=System.currentTimeMillis();
//		test.write2("test1", 10000);
//		System.out.println("方法2(1w):耗时:"+String.valueOf((System.currentTimeMillis()-st)));
//		st=System.currentTimeMillis();
//		test.write2("test1", 100000);
//		System.out.println("方法2(10w):耗时:"+String.valueOf((System.currentTimeMillis()-st)));
//		st=System.currentTimeMillis();
//		test.write2("test1", 1000000);
//		System.out.println("方法2(100w):耗时:"+String.valueOf((System.currentTimeMillis()-st)));
	}
}
