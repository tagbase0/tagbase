package com.oppo.tagbase.jobv2;

import com.google.common.base.Preconditions;
import com.google.common.collect.RangeSet;
import com.oppo.tagbase.dict.util.LocalDateTimeUtil;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobState;
import com.oppo.tagbase.meta.obj.Slice;
import com.oppo.tagbase.meta.obj.Task;
import com.oppo.tagbase.meta.obj.TaskState;
import com.oppo.tagbase.meta.util.RangeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.LocalDateTime;
import java.util.List;

import static com.oppo.tagbase.jobv2.JobErrorCode.DICT_NOT_CONTINUOUS;
import static com.oppo.tagbase.jobv2.JobErrorCode.JOB_OVERLAP;
import static com.oppo.tagbase.jobv2.JobErrorCode.SLICE_OVERLAP;
import static com.oppo.tagbase.jobv2.JobUtil.makeJobTimeline;
import static com.oppo.tagbase.jobv2.JobUtil.makeSliceTimeline;
import static com.oppo.tagbase.meta.obj.JobState.PENDING;
import static com.oppo.tagbase.meta.obj.JobState.RUNNING;
import static com.oppo.tagbase.meta.obj.JobState.SUCCESS;
import static java.lang.String.format;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class JobManager {

    private static final long MILLS_OF_DAY = 24 * 60 * 60 * 1000;

    private final Logger log = LoggerFactory.getLogger(getClass());

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
        for (Task task : job.getTasks()) {
            metadataJob.addTask(task);
        }

        return job;
    }


    private void checkArgs(LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {
        // check dataLowerTime < dataUpperTime
        Preconditions.checkArgument(dataLowerTime.isBefore(dataUpperTime), "lower time must before upper");

        // check dataUpperTime - dataLowerTime = n Days
        long diff = LocalDateTimeUtil.minus(dataLowerTime, dataLowerTime);
        Preconditions.checkArgument(diff == MILLS_OF_DAY, "build time range must be 1 Day");
    }

    private void checkTimeline(String dbName, String tableName, LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {

        // check job overlap

        List<Job> jobList = metadataJob.listNotCompletedJob(dbName, tableName, dataLowerTime, dataUpperTime);

        Timeline jobTimeline = makeJobTimeline(jobList);

        if (jobTimeline.intersects(RangeUtil.of(dataLowerTime, dataUpperTime))) {
            String message = "job overlap others, please adjust job time bounds or discard overlapped jobs.";
            log.error(message);
            throw new JobException(JOB_OVERLAP, message );
        }

        // check slice overlap


        // slice timeline : [2020-02-01, 2020-02-03) [2020-02-03, 2020-02-05) [2020-02-05, 2020-02-07)
        // 1. [2020-02-01, 2020-02-03) ok
        // 2. [2020-02-01, 2020-02-02) error
        // 3. [2020-02-01, 2020-02-05) ok
        // 4. [2020-02-07, 2020-02-09) ok
        // 5. [2020-02-15, 2020-02-17) ok
        List<Slice> sliceList = metadata.getIntersectionSlices(dbName, tableName, RangeUtil.of(dataLowerTime, dataUpperTime));
        Timeline sliceTimeline = makeSliceTimeline(sliceList);

        if (sliceTimeline.overlap(RangeUtil.of(dataLowerTime, dataUpperTime))) {
            String message = "job overlap other slices, please adjust job time bounds.";
            log.error(message);
            throw new JobException(SLICE_OVERLAP, message );
        }

        RangeSet<LocalDateTime> intersections = sliceTimeline.intersection(RangeUtil.of(dataLowerTime, dataUpperTime));

        if (!intersections.isEmpty()) {
            log.info("job will override {}.{} slices : {}", dbName, tableName, intersections);
        }

    }


    public Job buildDict(LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {

        // preconditions checking
        checkArgs(dataLowerTime, dataUpperTime);

        checkPendingLimit();

        checkDictJobContinuous(dataLowerTime);

        // create job
        Job job = JobUtil.newDictJob(dataLowerTime, dataUpperTime);

        // add to metadata
        metadataJob.addJob(job);
        for (Task task : job.getTasks()) {
            metadataJob.addTask(task);
        }

        return job;
    }

    /**
     * To avoid missing elements, there must be a successful or potential successful dict job
     * which on time bound is connected with the new one.
     * <p>
     * For example: if the latest successful or potential successful dict job is [2020-02-01, 2020-02-02)
     * you can not create dict job [2020-02-03, 2020-02-04).
     */
    private void checkDictJobContinuous(LocalDateTime dataLowerTime) {

        Job job = metadataJob.getLatestDictJob(PENDING, RUNNING, SUCCESS);
        if (job != null && !job.getDataUpperTime().equals(dataLowerTime)) {
            String message = format("dict not continuous at %s, " +
                    "pls make sure the previous dict job is successful or adjust the dict job time bound.", dataLowerTime);
            log.error(message);
            throw new JobException(DICT_NOT_CONTINUOUS, message);
        }

    }

    private void checkPendingLimit() {
        if (metadataJob.getPendingJobCount() > jobConf.getPendingLimit()) {
            String message = format("Pending job count reaches system limit %d", jobConf.getPendingLimit());
            log.error(message);
            throw new JobException(message);
        }
    }

    public JobState getJobState(String jobId) {
        return metadataJob.getJob(jobId).getState();
    }


    /**
     * resume a FAILED or SUSPEND job
     */
    public Job resumeJob(String jobId) {
        Job job = metadataJob.getJob(jobId);
        JobFSM jobFSM = JobFSM.of(job, metadataJob);
        jobFSM.toPending();

        job.getTasks().stream()
                .filter(task -> {
                    // because some tasks maybe transfer to next state.
                    log.info("skip resume task {} for it is in {} state", task.getName(), task.getState());
                    return task.getState() == TaskState.FAILED || task.getState() == TaskState.SUSPEND;
                })
                .forEach(task -> {
                    TaskFSM taskFSM = TaskFSM.of(task, metadataJob);
                    taskFSM.toPending();
                });

        return job;
    }

    public Job suspendJob(String jobId) {
        Job job = metadataJob.getJob(jobId);

        JobFSM jobFSM = JobFSM.of(job, metadataJob);
        jobFSM.toSuspend();

        job.getTasks().stream()
                .filter(task -> {
                    // because some tasks maybe transfer to next state.
                    log.info("skip suspend task {} for it is in {} state", task.getName(), task.getState());
                    return task.getState() == TaskState.RUNNING;
                })
                .forEach(task -> {
                    TaskFSM taskFSM = TaskFSM.of(task, metadataJob);
                    taskFSM.toSuspend();
                });

        return job;
    }

    public Job discardJob(String jobId) {
        Job job = metadataJob.getJob(jobId);

        JobFSM jobFSM = JobFSM.of(job, metadataJob);
        jobFSM.toDiscard();

        job.getTasks().stream()
                .filter(task -> {
                    // because some tasks maybe transfer to next state.
                    log.info("skip discard task {} for it is in {} state", task.getName(), task.getState());
                    return !task.getState().isCompleted();
                })
                .forEach(task -> {
                    TaskFSM taskFSM = TaskFSM.of(task, metadataJob);
                    taskFSM.toDiscard();
                });

        return job;
    }

    public void deleteJob(String jobId) {
        JobFSM jobFSM = JobFSM.of(jobId, metadataJob);
        jobFSM.delete();
    }

    public Job getJob(String jobId) {
        //TODO
        return null;
    }

    public Job listJob(String dbName, String tableName) {
        //TODO
        return null;
    }


    public Job listDictJob(String dbName, String tableName) {
        //TODO
        return null;
    }

    public Job listJob(String dbName, String tableName, LocalDateTime lowerCreateTime, LocalDateTime upperCreateTime) {
        //TODO
        return null;
    }
}
