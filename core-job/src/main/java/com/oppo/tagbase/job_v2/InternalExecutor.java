package com.oppo.tagbase.job_v2;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class InternalExecutor<V> implements Executor<Runnable> {

    @Override
    public void perform(Runnable task) throws Exception {
        task.run();
    }

}
