package com.oppo.tagbase.meta.type;

/**
 * Created by wujianchao on 2020/2/13.
 */
public enum  DataType {

    /**
     * dim type
     */
    STRING,
    /**
     * slice column type
     * format yyyyMMdd
     */
    DATE,

    /**
     * metric type
     */
    LONG,
    BITMAP
}
