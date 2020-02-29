package com.oppo.tagbase.jobv2.skeleton;

import com.oppo.tagbase.meta.obj.Task;

/**
 * Created by wujianchao on 2020/2/27.
 */
public abstract class LocalExecutable implements Runnable {

    private Task task;

    public LocalExecutable(Task task) {
        this.task = task;
    }

    public Task getTask() {
        return task;
    }


}
