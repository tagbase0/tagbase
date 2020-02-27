package com.oppo.tagbase.job_v2;

import com.oppo.tagbase.common.util.Preconditions;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Job;

import javax.inject.Inject;
import java.sql.Date;
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

    public Job build(String dbName, String tableName, Date dataLowerTime, Date dataUpperTime) {

        List<Job> jobList = metadataJob.listNotCompletedJob(dbName, tableName, dataLowerTime, dataUpperTime);
        check(dbName, tableName, dataLowerTime, dataUpperTime);
        return null;
    }

    private void check(String dbName, String tableName, Date dataLowerTime, Date dataUpperTime) {
        // check dataLowerTime < dataUpperTime
        Preconditions.check(dataLowerTime.before(dataUpperTime), "lower time must less than upper time");

        // check dataUpperTime - dataLowerTime = n Days
        long diff = dataUpperTime.getTime() - dataLowerTime.getTime();
//        Preconditions.check((diff % MILLS_OF_DAY == 0, "build time range must be n Days");

        // check time line
//        Timeline timeline = new Timeline();

    }

    public Job rebuild(String dbName, String tableName, Date dataLowerTime, Date dataUpperTime) {

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

    public Job listJob(String dbName, String tableName, Date startTime, Date endTime) {
        return null;
    }
}
