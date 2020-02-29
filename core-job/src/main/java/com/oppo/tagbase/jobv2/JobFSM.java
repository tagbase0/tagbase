package com.oppo.tagbase.jobv2;

import com.google.common.base.Preconditions;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobState;

/**
 * Created by wujianchao on 2020/2/29.
 */
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

    public void toPending() {
        Preconditions.checkArgument(job.getState() == JobState.FAILED
                || job.getState() == JobState.SUSPEND);
        job.setState(JobState.PENDING);
        metadataJob.updateJobStatus(job.getId(), job.getState());
    }

    public void toRunning() {
        Preconditions.checkArgument(job.getState() == JobState.PENDING);
        job.setState(JobState.RUNNING);
        metadataJob.updateJobStatus(job.getId(), job.getState());
    }

    public void toFailed() {
        Preconditions.checkArgument(job.getState() == JobState.RUNNING);
        job.setState(JobState.FAILED);
        metadataJob.updateJobStatus(job.getId(), job.getState());
    }

    public void toSuspend() {
        Preconditions.checkArgument(job.getState() == JobState.RUNNING);
        job.setState(JobState.FAILED);
        metadataJob.updateJobStatus(job.getId(), job.getState());
    }

    public void toSuccess() {
        Preconditions.checkArgument(job.getState() == JobState.RUNNING);
        job.setState(JobState.SUCCESS);
        metadataJob.updateJobStatus(job.getId(), job.getState());
    }

    public void toDiscard() {
        Preconditions.checkArgument(job.getState() == JobState.SUSPEND
                || job.getState() == JobState.FAILED);
        job.setState(JobState.DISCARD);
        metadataJob.updateJobStatus(job.getId(), job.getState());
    }

}
