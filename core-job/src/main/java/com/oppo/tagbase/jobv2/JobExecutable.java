package com.oppo.tagbase.jobv2;

import com.oppo.tagbase.meta.obj.Job;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class JobExecutable {


    private Job job;

    ExecutableChain taskChain;

    public JobExecutable(Job job) {
        this.job = job;
    }

    public void perform() {
        taskChain.perform();
    }

}
