package com.agilor.distribute.test;

import agilor.distributed.communication.client.Value;
import com.agilor.distribute.client.nameManage.AgilorDistributeClient;



public class ClientTestMain {

	public static void  addTagTest(){
        long st=System.currentTimeMillis();
        AgilorDistributeClient test=new AgilorDistributeClient();
        for(int i=0;i<100;i++)
            test.createTagNode("Simu1."+String.valueOf(i),new Value(Value.Types.FLOAT));
        System.out.println("方法1(1w):耗时:"+String.valueOf((System.currentTimeMillis()-st)));
    }
    public static void writeValue(){
        long st=System.currentTimeMillis();
        AgilorDistributeClient test=new AgilorDistributeClient();
        for(int j=0;j<100;j++)
            for(int i=0;i<100;i++){
                Value tmp=new Value(Value.Types.FLOAT);
                tmp.setFvalue(i+(float)12.34);
                test.write("Simu1."+String.valueOf(i),tmp);
            }
        System.out.println("方法1(1w):耗时:"+String.valueOf((System.currentTimeMillis()-st)));
    }
	public static void main(String[] args) {
		// TODO Auto-generated method stub
        writeValue();
        //addTagTest();

//		String hostPort = "101.200.77.14:2181";
//		ClientNodeHandler test1=new ClientNodeHandler(hostPort,"/ClientNodeInfo");
//		test1.stratHandler();

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
