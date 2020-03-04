package com.oppo.tagbase.jobv2;

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

    public boolean isRunning() {
        return task.getState() == TaskState.RUNNING;
    }

    public void toPending() {
        JobPreconditions.checkState(task.getState() == TaskState.FAILED);
        metadataJob.updateTaskStatus(task.getId(), task.getState());
        task.setState(TaskState.PENDING);
    }

    public void toRunning() {
        JobPreconditions.checkState(task.getState() == TaskState.PENDING);
        metadataJob.updateTaskStatus(task.getId(), task.getState());
        task.setState(TaskState.RUNNING);
    }
    public void toSuspend() {
        JobPreconditions.checkState(task.getState() == TaskState.RUNNING);
        metadataJob.updateTaskStatus(task.getId(), task.getState());
        task.setState(TaskState.SUSPEND);
    }

    public void toFailed() {
        JobPreconditions.checkState(task.getState() == TaskState.RUNNING);
        metadataJob.updateTaskStatus(task.getId(), task.getState());
        task.setState(TaskState.FAILED);
    }

    public void toSuccess() {
        JobPreconditions.checkState(task.getState() == TaskState.RUNNING);
        metadataJob.updateTaskStatus(task.getId(), task.getState());
        task.setState(TaskState.SUCCESS);
    }

    public void toDiscard() {
        JobPreconditions.checkState(task.getState() == TaskState.SUSPEND
                        || task.getState() == TaskState.RUNNING
                        || task.getState() == TaskState.FAILED
                        || task.getState() == TaskState.PENDING,
                "task already completed");
        metadataJob.updateTaskStatus(task.getId(), task.getState());
        task.setState(TaskState.DISCARD);
    }

}
