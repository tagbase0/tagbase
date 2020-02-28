package com.oppo.tagbase.jobv2;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class LocalExecutor implements Executor<Runnable> {

    @Override
    public void perform(Runnable task) throws Exception {
        task.run();
    }

}
