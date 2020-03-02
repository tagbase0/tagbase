package com.oppo.tagbase.jobv2;

import com.oppo.tagbase.common.TagbaseException;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Task;

import java.util.concurrent.Callable;

/**
 * Created by wujianchao on 2020/3/2.
 */
public class TaskExecutable implements Executable {

    private Task task;
    private Callable<Void> delegate;
    private MetadataJob metadataJob;


    public TaskExecutable(Task task, MetadataJob metadataJob, Callable<Void> delegate) {
        this.task = task;
        this.delegate = delegate;
        this.metadataJob = metadataJob;
    }

    @Override
    public void perform() throws JobException {
        TaskFSM taskFSM = TaskFSM.of(task, metadataJob);
        try {
            taskFSM.toRunning();
            delegate.call();
            taskFSM.toSuccess();
        } catch (Exception e) {
            taskFSM.toFailed();
            if(!(e instanceof TagbaseException)) {
                throw new JobException(e, "Task %s failed", task.getName());
            }
            throw (TagbaseException)e;
        }
    }

}
