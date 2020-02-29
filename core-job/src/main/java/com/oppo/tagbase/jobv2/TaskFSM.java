package com.oppo.tagbase.jobv2;

import com.google.common.base.Preconditions;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Task;
import com.oppo.tagbase.meta.obj.TaskState;

/**
 * Created by wujianchao on 2020/2/29.
 */
public final class TaskFSM {

    private Task task;
    private MetadataJob metadataJob;

    private TaskFSM(Task task, MetadataJob metadataJob) {
        this.task = task;
        this.metadataJob = metadataJob;
    }

    public static TaskFSM of(Task task, MetadataJob metadataJob) {
        return new TaskFSM(task, metadataJob);
    }

    public void toPending() {
        Preconditions.checkArgument(task.getState() == TaskState.FAILED
                || task.getState() == TaskState.SUSPEND);
        task.setState(TaskState.PENDING);
        metadataJob.updateTaskStatus(task.getId(), task.getState());
    }

    public void toRunning() {
        Preconditions.checkArgument(task.getState() == TaskState.PENDING);
        task.setState(TaskState.RUNNING);
        metadataJob.updateTaskStatus(task.getId(), task.getState());
    }

    public void toFailed() {
        Preconditions.checkArgument(task.getState() == TaskState.RUNNING);
        task.setState(TaskState.FAILED);
        metadataJob.updateTaskStatus(task.getId(), task.getState());
    }

    public void toSuspend() {
        Preconditions.checkArgument(task.getState() == TaskState.RUNNING);
        task.setState(TaskState.FAILED);
        metadataJob.updateTaskStatus(task.getId(), task.getState());
    }

    public void toSuccess() {
        Preconditions.checkArgument(task.getState() == TaskState.RUNNING);
        task.setState(TaskState.SUCCESS);
        metadataJob.updateTaskStatus(task.getId(), task.getState());
    }

    public void toDiscard() {
        Preconditions.checkArgument(task.getState() == TaskState.SUSPEND
                || task.getState() == TaskState.FAILED);
        task.setState(TaskState.DISCARD);
        metadataJob.updateTaskStatus(task.getId(), task.getState());
    }

}
