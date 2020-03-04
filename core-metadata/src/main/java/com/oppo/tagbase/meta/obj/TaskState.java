package com.oppo.tagbase.meta.obj;

/**
 * Created by wujianchao on 2020/2/17.
 */
public enum TaskState {
    PENDING,
    RUNNING,
    SUSPEND,
    FAILED,
    SUCCESS,
    DISCARD
    ;

    public boolean isCompleted() {
        return this == SUCCESS;
    }
}
