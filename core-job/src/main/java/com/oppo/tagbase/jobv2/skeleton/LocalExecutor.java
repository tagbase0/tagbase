package com.oppo.tagbase.jobv2.skeleton;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class LocalExecutor implements Executor<Runnable> {

    @Override
    public void submit(Runnable task) throws Exception {
        task.run();
    }

}
