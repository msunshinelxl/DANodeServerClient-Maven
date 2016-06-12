package com.agilor.distribute.testZK;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Created by xinlongli on 16/5/30.
 */
public class CountLatch {
    public interface Afterwait{
        public void todo();
    }
    private volatile long count=0;
    private final Object lock=new Object();
    Afterwait afterwait=null;
    public CountLatch(long total){
        count=total;
    }
    public void await()throws InterruptedException{
        if(count!=0){
            synchronized (lock){
                if(count!=0){
                    lock.wait();
                    if(afterwait!=null){
                        afterwait.todo();
                    }
                }
            }
        }
    }

    public void setAfterWait(Afterwait aw){
        this.afterwait=aw;
    }

    public void countDown(){
        synchronized (lock){
            //if(count!=0)
            count-=1;
            if(count==0)
                lock.notifyAll();
            //System.out.println("-"+count);
        }
    }

    public void countAdd(){
        synchronized (lock){
            count+=1;
            //System.out.println("+"+count);
        }
    }

    public static void main(String [] args){
        final CountLatch count=new CountLatch(0);
        Thread mainrunner=new Thread(()->{
            for(int i=0;i<10000;i++){
                try{

                    //System.out.println(i);
                    count.await();
                    //Thread.sleep(1);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        });
        mainrunner.start();
        CountDownLatch countDown=new CountDownLatch(10);
        for(int i=0;i<10;i++){
            final int number=i;
            Thread tmprunner=new Thread(()->{
                try{
                    countDown.await();
                    count.countAdd();
                    Random random=new Random();
                    //System.out.println("running : " + number);
                    //Thread.sleep(random.nextInt(10));
                    count.countDown();
                }catch (Exception e){
                    e.printStackTrace();
                }
            });
            tmprunner.start();
            countDown.countDown();
        }
    }
}
