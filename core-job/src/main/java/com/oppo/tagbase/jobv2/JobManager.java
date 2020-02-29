package com.oppo.tagbase.jobv2;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.oppo.tagbase.common.util.LocalDateTimeUtil;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.*;

import javax.inject.Inject;
import java.time.LocalDateTime;
import java.util.List;

import static com.oppo.tagbase.jobv2.JobErrorCode.JOB_OVERLAP;
import static com.oppo.tagbase.jobv2.JobErrorCode.SLICE_OVERLAP;
import static com.oppo.tagbase.jobv2.JobUtil.*;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class JobManager {

    private static final long MILLS_OF_DAY = 24 * 60 * 60 * 1000;

    @Inject
    private Metadata metadata;

    @Inject
    private MetadataJob metadataJob;


    public Job build(String dbName, String tableName, LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {

        // preconditions checking
        checkNewDataJob(dbName, tableName, dataLowerTime, dataUpperTime);

        // create job
        Job job = new Job();
        job.setId(JobIdGenerator.nextId());
        job.setName(JobUtil.makeDataJobName(dbName, tableName, dataLowerTime, dataUpperTime));
        job.setDbName(dbName);
        job.setTableName(tableName);
        job.setDataLowerTime(dataLowerTime);
        job.setDataUpperTime(dataUpperTime);
        job.setState(JobState.PENDING);
        job.setStartTime(LocalDateTime.now());
        job.setType(JobType.DATA);

        addTasksToJob(job);

        // add to metadata
        metadataJob.addJob(job);
        for(Task task : job.getTasks()) {
            metadataJob.addTask(task);
        }

        return job;
    }


    private void checkNewDataJob(String dbName, String tableName, LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {
        // check dataLowerTime < dataUpperTime
        Preconditions.checkArgument(dataLowerTime.isBefore(dataUpperTime), "lower time must less than upper time");

        // check dataUpperTime - dataLowerTime = n Days
        long diff = LocalDateTimeUtil.minus(dataLowerTime, dataLowerTime);
        Preconditions.checkArgument((diff % MILLS_OF_DAY == 0), "build time range must be n Days");

        // check job overlap
        List<Job> jobList = metadataJob.listNotCompletedJob(dbName, tableName, dataLowerTime, dataUpperTime);
        Timeline jobTimeline = makeJobTimeline(jobList);

        if(jobTimeline.intersects(Range.closedOpen(dataLowerTime, dataUpperTime))) {
            throw new JobException(JOB_OVERLAP, "job overlap others, please adjust job time bounds or delete overlapped jobs.");
        }

        // check segment overlap

        List<Slice> sliceList = metadata.getSlices(dbName, tableName, Range.closedOpen(dataLowerTime, dataUpperTime));
        Timeline sliceTimeline = makeSliceTimeline(sliceList);

        if(sliceTimeline.overlap(Range.closedOpen(dataLowerTime, dataUpperTime))) {
            throw new JobException(SLICE_OVERLAP, "job overlap other slices, please adjust job time bounds.");
        }

    }


    public Job buildDict(LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {

        // create job
        Job job = new Job();
        job.setName(JobUtil.makeDictJobName(dataLowerTime, dataUpperTime));
        job.setId(JobIdGenerator.nextId());
        job.setState(JobState.PENDING);
        job.setCreateTime(LocalDateTime.now());
        job.setDataLowerTime(dataLowerTime);
        job.setDataUpperTime(dataUpperTime);
        job.setType(JobType.DATA);

        addTasksToJob(job);

        // add to metadata
        metadataJob.addJob(job);
        for(Task task : job.getTasks()) {
            metadataJob.addTask(task);
        }

        return job;
    }

    public JobState getJobState(String jobId) {
        return metadataJob.getJob(jobId).getState();
    }

    public Job rebuild(String dbName, String tableName, LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {
        //TODO
        return null;
    }

    //TODO
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
