package com.oppo.tagbase.jobv2;

import com.oppo.tagbase.jobv2.skeleton.Executable;
import com.oppo.tagbase.meta.obj.Job;

import java.util.List;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class JobExecutable {


    private Job job;

    private List<Executable> taskChain;

    public JobExecutable(Job job, List<Executable> taskChain) {
        this.job = job;
        this.taskChain = taskChain;
    }

    public void perform() {
        for(Executable task : taskChain) {
            task.perform();
        }
    }

}
