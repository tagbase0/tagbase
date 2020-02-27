package com.oppo.tagbase.job.engine.obj;

/**
 * Created by wujianchao on 2020/2/17.
 */
public enum TaskState {
    PENDING,
    RUNNING,
    FAILED,
    SUCCESS,
    KILLED,
    PREP,
    UNKNOWN
}
