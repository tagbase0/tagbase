package com.oppo.tagbase.jobv2;

import com.google.common.util.concurrent.*;
import com.google.inject.Inject;
import com.oppo.tagbase.common.guice.LifecycleStart;
import com.oppo.tagbase.common.guice.ExtensionImpl;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobType;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by wujianchao on 2020/2/27.
 */
@ExtensionImpl(name = "singleton", extensionPoint = Scheduler.class)
public class SingletonScheduler implements Scheduler {

    private Logger log = LoggerFactory.getLogger(getClass());

    @Inject
    private MetadataJob metadataJob;
    @Inject
    private SingletonSchedulerConfig config;
    @Inject
    private BatchBuildingJobMaker batchBuildingJobMaker;

    private AtomicInteger running = new AtomicInteger(0);

    private ListeningExecutorService jobExecutor;
    private ScheduledExecutorService jobScheduler;

    @Override
    @LifecycleStart
    public void schedule() {
        log.info("starting SingletonScheduler");

        ThreadFactory executorThreadNaming = new ThreadFactoryBuilder()
                .setNameFormat("job-executor-%d").build();
        jobExecutor = MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(config.getParallelism(), executorThreadNaming));

        jobScheduler = Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "job-scheduler")
        );

        // schedule task
        jobScheduler.scheduleAtFixedRate(() -> {
            // get pending jobs
            List<Job> jobList = metadataJob.listPendingJobs();
            log.debug("taking {} pending jobs from metadata", jobList.size());

            List<JobExecutable> jobExecutableList = jobList.stream()
                    //dict building job prior to data job
                    .sorted()
                    // keep only one dictionary jon running.
                    .filter(job -> JobType.DICTIONARY == job.getType() && metadataJob.getRunningDictJob() != null)
                    .map(job -> batchBuildingJobMaker.make(job))
                    .collect(Collectors.toList());

            for (JobExecutable jobExecutable : jobExecutableList) {

                if (running.get() > config.getParallelism()) {
                    break;
                }

                running.incrementAndGet();
                ListenableFuture ret = jobExecutor.submit(jobExecutable::perform);

                Futures.addCallback(ret, new FutureCallback() {

                    @Override
                    public void onSuccess(@Nullable Object result) {
                        running.decrementAndGet();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        running.decrementAndGet();
                    }
                }, Runnable::run);
            }


        }, config.getInterval(), config.getInterval(), TimeUnit.SECONDS);

    }

    @Override
    @LifecycleStart
    public void shutdown() {
        log.info("shutting down SingletonScheduler");

        running.set(0);

        jobScheduler.shutdown();
        jobExecutor.shutdown();
    }
}
