package com.agilor.distribute.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogTestMain {
	    volatile int n;
	    volatile boolean valueSet = false;
	    public boolean stop=false;
	    Object readLock=new Object();
	    Object writeLock=new Object();
	    int get(int i) {
	    	synchronized (readLock) {
	        if (!valueSet)
	            try {
	            	readLock.wait();
	            } catch (InterruptedException e) {
	                System.out.println("InterruptedException caught");
	            }
		        if(n!=i)
		        	System.out.println("error"+i+"  "+n);
		        else
		        	System.out.println("n : "+n+" ; i : "+i);
		        valueSet = false;
	    	}
	        synchronized (writeLock) {
	        	writeLock.notify();
	        }
	        return n;
	    }

	    void put() {
        	synchronized(writeLock){
		        if (valueSet){
		            try {
		            	writeLock.wait();
		            } catch (InterruptedException e) {
		                System.out.println("InterruptedException caught");
		            }	
		        }
		        valueSet = true;
		        this.n=n+1;
		        if(this.n>1000)
		        	stop=true;
	        }
	        try {
				Thread.sleep(20);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
//	        System.out.println("Put: " + n);
	        synchronized (readLock) {
	        	readLock.notify();
	        }
	    }
	
	public class Producer1 implements Runnable {
	    
	    private LogTestMain q;

	    Producer1(LogTestMain q) {
	        this.q = q;
	    }

	    public void run() {
	        int i = 0;
	        while (q.stop==false) {
	            q.put();
	        }
	    }

	}
//	public class Producer2 implements Runnable {
//	    
//	    private LogTestMain q;
//
//	    Producer2(LogTestMain q) {
//	        this.q = q;
//	        new Thread(this, "Producer").start();
//	    }
//
//	    public void run() {
//	        int i = 0;
//	        while (true) {
//	            q.put(i--);
//	        }
//	    }
//
//	}
	public class Consumer implements Runnable {
	    
	    private LogTestMain q;

	    Consumer(LogTestMain q) {
	        this.q = q;
	        
	    }

	    public void run() {
	    	int i=0;
	        while (q.stop==false) {
	            q.get(++i);
	        }
	    }

	}
//	volatile int bufferSize;
//	static final int MaxBuffer=10;
//	synchronized int get(){
//		if(bufferSize>0){
//			try {
//				wait();
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//		
//		
//	}
	
    public static void main(String[] args) {
    	LogTestMain q = new LogTestMain(); 
    	long st=System.currentTimeMillis();
    	
    	Thread t1=new Thread(q.new Producer1(q), "Producer");
    	Thread t2=new Thread(q.new Producer1(q), "Producer");
    	Thread t3=new Thread(q.new Producer1(q), "Producer");
    	Thread t4=new Thread(q.new Producer1(q), "Producer");
    	Thread t5=new Thread(q.new Producer1(q), "Producer");
        Thread t6=new Thread(q.new Consumer(q), "Consumer");
        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();
        t6.start();
        try {
			t1.join();
			t2.join();
			t3.join();
			t4.join();
			t5.join();
			t6.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println(System.currentTimeMillis()-st); 
    }

}
