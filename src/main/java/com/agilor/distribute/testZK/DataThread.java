package com.agilor.distribute.testZK;

import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;

/**
 * Created by xinlongli on 16/6/1.
 */
public class DataThread extends Thread {
    public interface Runner{
        void beforRun(DataThread pa);
        void running(DataThread pa);
        void afterRun(DataThread pa);
    }
    Runner runner;
    protected boolean stat=false;
    DistributedDoubleBarrier barrier=null;
    public DataThread(DistributedDoubleBarrier barrier, Runner runner){
        stat=true;
        this.barrier=barrier;
        this.runner=runner;
    }

    public void run(){
        runner.beforRun(this);
        try{
        barrier.enter();
        }catch(Exception e){
            e.printStackTrace();
        }
        runner.running(this);
        try{
            barrier.leave();
        }catch(Exception e){
            e.printStackTrace();
        }
        runner.afterRun(this);

    }
    synchronized public void myStop() throws Exception{
        stat=false;
    }


}
