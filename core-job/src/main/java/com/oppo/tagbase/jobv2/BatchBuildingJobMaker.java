package com.oppo.tagbase.jobv2;

import com.oppo.tagbase.meta.obj.Job;

import static com.oppo.tagbase.meta.obj.JobType.DICTIONARY;

/**
 * Created by wujianchao on 2020/2/27.
 */
public final class BatchBuildingJobMaker {

    public static JobExecutable make(Job job) {
        if(job.getType() == DICTIONARY) {
            return new DictJobExecutableMaker().make(job);
        }
        return new DataJobExecutableMaker().make(job);
    }

}
