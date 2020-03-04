package com.oppo.tagbase.jobv2;

import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobState;

/**
 * Created by wujianchao on 2020/2/29.
 */
//TODO redefine FSM with diagram
public final class JobFSM {

    private Job job;
    private MetadataJob metadataJob;

    private JobFSM(Job job, MetadataJob metadataJob) {
        this.job = job;
        this.metadataJob = metadataJob;
    }

    public static JobFSM of(Job job, MetadataJob metadataJob) {
        return new JobFSM(job, metadataJob);
    }

    public static JobFSM of(String jobId, MetadataJob metadataJob) {
        return of(metadataJob.getJob(jobId), metadataJob);
    }

    public boolean isRunning() {
        return job.getState() == JobState.RUNNING;
    }

    public void toPending() {
        JobPreconditions.checkState(job.getState() == JobState.FAILED
                || job.getState() == JobState.SUSPEND);
        metadataJob.updateJobStatus(job.getId(), job.getState());
        job.setState(JobState.PENDING);
    }

    public void toRunning() {
        JobPreconditions.checkState(job.getState() == JobState.PENDING);
        metadataJob.updateJobStatus(job.getId(), job.getState());
        job.setState(JobState.RUNNING);
    }

    public void toFailed() {
        JobPreconditions.checkState(job.getState() == JobState.RUNNING);
        metadataJob.updateJobStatus(job.getId(), job.getState());
        job.setState(JobState.FAILED);
    }

    public void toSuspend() {
        JobPreconditions.checkState(job.getState() == JobState.RUNNING);
        metadataJob.updateJobStatus(job.getId(), job.getState());
        job.setState(JobState.FAILED);
    }

    public void toSuccess() {
        JobPreconditions.checkState(job.getState() == JobState.RUNNING);
        metadataJob.updateJobStatus(job.getId(), job.getState());
        job.setState(JobState.SUCCESS);
    }

    public void toDiscard() {
        JobPreconditions.checkState(job.getState() == JobState.SUSPEND
                || job.getState() == JobState.RUNNING
                || job.getState() == JobState.FAILED
                || job.getState() == JobState.PENDING,
                "job already completed");
        metadataJob.updateJobStatus(job.getId(), job.getState());
        job.setState(JobState.DISCARD);
    }

    public void delete() {
        JobPreconditions.checkState(job.getState().isCompleted(), "can not delete an uncompleted job");
        metadataJob.deleteJob(job.getId());
    }

}
