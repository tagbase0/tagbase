package com.oppo.tagbase.storage.core.example.testobj;

/**
 * Created by wujianchao on 2020/2/5.
 */
public enum SliceStatus {
    /**
     * slices who can serve a query
     */
    READY,

    /**
     * slices who can back to ready status
     */
    DISABLED,

    /**
     * slices who can dropped
     */
    DROPPED
}
