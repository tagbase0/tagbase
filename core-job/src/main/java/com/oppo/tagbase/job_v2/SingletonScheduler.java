package com.oppo.tagbase.job_v2;

import com.google.inject.Inject;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by wujianchao on 2020/2/27.
 */
public class SingletonScheduler implements Scheduler {

    //TODO add nThreads config
    private ExecutorService executorService = Executors.newFixedThreadPool(10);

    @Inject
    JobManager jobManager;

    @Override
    public void schedule() {

    }

    @Override
    public void shutdown() {

    }
}
