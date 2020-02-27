package com.oppo.tagbase.job_v2;

import com.google.common.collect.Lists;
import com.oppo.tagbase.common.util.LocalDateTimeUtil;
import com.oppo.tagbase.common.util.Preconditions;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobState;
import com.oppo.tagbase.meta.obj.Task;

import javax.inject.Inject;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class JobManager {

    private static final long MILLS_OF_DAY = 24 * 60 * 60 * 1000;

    private PriorityBlockingQueue<Job> running = new PriorityBlockingQueue<>();
    private PriorityBlockingQueue<Job> pending = new PriorityBlockingQueue<>();


    @Inject
    private MetadataJob metadataJob;

    public Job build(String dbName, String tableName, LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {

        // check preconditions
        check(dbName, tableName, dataLowerTime, dataUpperTime);

        // submit job
        Job job = new Job();
        job.setId(JobIdGenerator.nextId());
        job.setDbName(dbName);
        job.setTableName(tableName);
        job.setDataLowerTime(dataLowerTime);
        job.setDataUpperTime(dataUpperTime);
        job.setState(JobState.PENDING);
        job.setStartTime(LocalDateTime.now());

        // add job tasks
        List<Task> taskList = Lists.newArrayList();
        job.setTasks(taskList);
        job.setLatestTask(taskList.get(0).getId());

        // makeJobExecutable

        metadataJob.addJob(job);
        for(Task task : taskList) {
            metadataJob.addTask(task);
        }

        return job;
    }


    private JobExecutable makeJobExecutable(Job job) {
        return null;
    }

    private void check(String dbName, String tableName, LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {
        // check dataLowerTime < dataUpperTime
        Preconditions.check(dataLowerTime.isBefore(dataUpperTime), "lower time must less than upper time");

        // check dataUpperTime - dataLowerTime = n Days
        long diff = LocalDateTimeUtil.minus(dataLowerTime, dataLowerTime);
        Preconditions.check((diff % MILLS_OF_DAY == 0), "build time range must be n Days");

        // check time line
        List<Job> jobList = metadataJob.listNotCompletedJob(dbName, tableName, dataLowerTime, dataUpperTime);
        //TODO
    }

    public Job rebuild(String dbName, String tableName, LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {

        check(dbName, tableName, dataLowerTime, dataUpperTime);
        return null;
    }

    public Job resumeJob(String jobId) {
        return null;
    }

    public Job stopJob(String jobId) {
        return null;
    }

    public Job deleteJob(String jobId) {
        return null;
    }

    public Job getJob(String jobId) {
        return null;
    }

    public Job listJob(String dbName, String tableName) {
        return null;
    }

    public Job listJob(String dbName, String tableName, LocalDateTime startTime, LocalDateTime endTime) {
        return null;
    }
}
