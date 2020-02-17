package com.oppo.tagbase.meta.obj;

/**
 * Created by wujianchao on 2020/2/5.
 */
public enum SliceStatus {
    /**
     * Slice is in building progress, which can not serve a query
     */
    BUILDING,
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
