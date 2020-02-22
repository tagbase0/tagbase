package com.oppo.tagbase.common;

/**
 * Created by wujianchao on 2020/2/22.
 */
public interface ErrorCode {

    /**
     * Get the associated error code
     * @return the error code
     */
    int getErrorCode();

    /**
     * Get the class of error code
     * @return the class of error code
     */
    Family getFamily();

    /**
     * Get the reason
     * @return the reason
     */
    String getReason();

    enum Family {
        METADATA,
        DICTIONARY,
        STORAGE,
        QUERY,
        JOB,
        OTHER
    }
}
