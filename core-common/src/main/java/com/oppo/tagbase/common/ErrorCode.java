package com.oppo.tagbase.common;

/**
 * Created by wujianchao on 2020/2/22.
 */
public interface ErrorCode {


    /**
     * Get the associated error code
     * @return the error code
     */
    int getCode();

    String getName();

    /**
     * Get the class of error code
     * @return the class of error code
     */
    Family getFamily();

    enum Family {
        METADATA,
        DICTIONARY,
        STORAGE,
        QUERY,
        JOB,
        OTHER
    }

    default String format() {
        return String.format("[ %s(%d) - %s ]", getName(), getCode(), getFamily().name());
    }
}
