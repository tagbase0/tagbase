package com.oppo.tagbase.meta.obj;

/**
 * Created by wujianchao on 2020/2/17.
 */
public enum JobState {
    PENDING,
    RUNNING,
    FAILED,
    SUSPEND,
    DISCARD,
    SUCCESS
    ;

    public boolean isCompleted() {
        return this == DISCARD || this == SUCCESS;
    }
}
