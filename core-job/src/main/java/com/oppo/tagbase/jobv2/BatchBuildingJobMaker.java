package com.oppo.tagbase.jobv2;

import com.oppo.tagbase.meta.obj.Job;

import javax.inject.Inject;

import static com.oppo.tagbase.meta.obj.JobType.DICTIONARY;

/**
 * Created by wujianchao on 2020/2/27.
 */
public final class BatchBuildingJobMaker {

    @Inject
    private DataJobExecutableMaker dataJobExecutableMaker;

    @Inject
    private DictJobExecutableMaker dictJobExecutableMaker;

    public JobExecutable make(Job job) {
        if(job.getType() == DICTIONARY) {
            return dictJobExecutableMaker.make(job);
        }
        return dataJobExecutableMaker.make(job);
    }

}
