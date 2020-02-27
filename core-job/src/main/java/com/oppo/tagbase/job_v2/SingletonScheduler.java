package com.oppo.tagbase.job_v2;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.oppo.tagbase.common.guice.LifecycleStart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Created by wujianchao on 2020/2/27.
 */
public class SingletonScheduler implements Scheduler {

    private Logger log = LoggerFactory.getLogger(getClass());


    private ThreadFactory executorThreadNaming = new ThreadFactoryBuilder().setNameFormat("job-executor-%d").build();

    //TODO add nThreads config
    private ExecutorService jobExecutor = Executors.newFixedThreadPool(10, executorThreadNaming);

    private ScheduledExecutorService jobScheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "job-scheduler")
    );

    @Inject
    JobManager jobManager;

    @Override
    @LifecycleStart
    public void schedule() {
        log.info("starting SingletonScheduler");
        jobScheduler.scheduleAtFixedRate(() -> {
            // get pending jobs
            List<JobExecutable> jobsList = null;

            // execute
            for(JobExecutable job : jobsList) {
                jobExecutor.submit(() -> {
                    job.perform();
                });
            }
        }, 10, 5, TimeUnit.SECONDS);

    }

    @Override
    @LifecycleStart
    public void shutdown() {
        log.info("shutting down SingletonScheduler");
        jobScheduler.shutdown();
        jobExecutor.shutdown();
    }
}
