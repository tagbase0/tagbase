package com.oppo.tagbase.jobv2;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.oppo.tagbase.common.util.LocalDateTimeUtil;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobState;
import com.oppo.tagbase.meta.obj.Slice;
import com.oppo.tagbase.meta.obj.Task;
import com.oppo.tagbase.meta.util.RangeUtil;

import javax.inject.Inject;
import java.time.LocalDateTime;
import java.util.List;

import static com.oppo.tagbase.jobv2.JobErrorCode.DICT_NOT_CONSISTENT;
import static com.oppo.tagbase.jobv2.JobErrorCode.JOB_OVERLAP;
import static com.oppo.tagbase.jobv2.JobErrorCode.SLICE_OVERLAP;
import static com.oppo.tagbase.jobv2.JobUtil.makeJobTimeline;
import static com.oppo.tagbase.jobv2.JobUtil.makeSliceTimeline;
import static com.oppo.tagbase.meta.obj.JobState.PENDING;
import static com.oppo.tagbase.meta.obj.JobState.RUNNING;
import static com.oppo.tagbase.meta.obj.JobState.SUCCESS;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class JobManager {

    private static final long MILLS_OF_DAY = 24 * 60 * 60 * 1000;

    @Inject
    private Metadata metadata;

    @Inject
    private MetadataJob metadataJob;


    @Inject
    private JobConfig jobConf;

    public Job build(String dbName, String tableName, LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {

        // preconditions checking
        checkArgs(dataLowerTime, dataUpperTime);

        checkPendingLimit();

        checkTimeline(dbName, tableName, dataLowerTime, dataUpperTime);

        // create job
        Job job = JobUtil.newDataJob(dbName, tableName, dataLowerTime, dataUpperTime);

        // add to metadata
        metadataJob.addJob(job);
        for(Task task : job.getTasks()) {
            metadataJob.addTask(task);
        }

        return job;
    }


    private void checkArgs(LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {
        // check dataLowerTime < dataUpperTime
        Preconditions.checkArgument(dataLowerTime.isBefore(dataUpperTime), "lower time must before upper");

        // check dataUpperTime - dataLowerTime = n Days
        long diff = LocalDateTimeUtil.minus(dataLowerTime, dataLowerTime);
        Preconditions.checkArgument(diff != MILLS_OF_DAY, "build time range must be 1 Day");
    }

    private void checkTimeline(String dbName, String tableName, LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {

        // check job overlap
        List<Job> jobList = metadataJob.listNotCompletedJob(dbName, tableName, dataLowerTime, dataUpperTime);
        Timeline jobTimeline = makeJobTimeline(jobList);

        if(jobTimeline.intersects(RangeUtil.of(dataLowerTime, dataUpperTime))) {
            throw new JobException(JOB_OVERLAP, "job overlap others, please adjust job time bounds or delete overlapped jobs.");
        }

        // check segment overlap

        List<Slice> sliceList = metadata.getIntersectionSlices(dbName, tableName, RangeUtil.of(dataLowerTime, dataUpperTime));
        Timeline sliceTimeline = makeSliceTimeline(sliceList);

        if(sliceTimeline.overlap(RangeUtil.of(dataLowerTime, dataUpperTime))) {
            throw new JobException(SLICE_OVERLAP, "job overlap other slices, please adjust job time bounds.");
        }

    }


    public Job buildDict(LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {

        // preconditions checking
        checkArgs(dataLowerTime, dataUpperTime);

        checkPendingLimit();

        checkDictJobConsistency(dataLowerTime);

        // create job
        Job job = JobUtil.newDictJob(dataLowerTime, dataUpperTime);

        // add to metadata
        metadataJob.addJob(job);
        for(Task task : job.getTasks()) {
            metadataJob.addTask(task);
        }

        return job;
    }

    private void checkDictJobConsistency(LocalDateTime dataLowerTime) {
        Job job = metadataJob.getLatestDictJob(Lists.newArrayList(PENDING, RUNNING, SUCCESS));
        if(job != null && !job.getDataUpperTime().equals(dataLowerTime)) {
            throw new JobException(DICT_NOT_CONSISTENT, "dict not consistent at %s, " +
                    "pls make sure the previous DICT job is successful or adjust the DICT job time bound.", dataLowerTime);
        }

    }

    private void checkPendingLimit() {
            if(metadataJob.getPendingJobCount() > jobConf.getPendingLimit()) {
            throw new JobException("Pending job count approaches system limit %d", jobConf.getPendingLimit());
        }
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
