package com.oppo.tagbase.job_v2;

import com.oppo.tagbase.meta.obj.Task;

/**
 * Created by wujianchao on 2020/2/27.
 */
public abstract class LocalExecutable implements Executable, Runnable {

    private Task task;

    public LocalExecutable(Task task) {
        this.task = task;
    }

    @Override
    public Task getTask() {
        return task;
    }


}
