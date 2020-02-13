package com.oppo.tagbase.common.util;

/**
 * Created by wujianchao on 2020/2/10.
 */
public class RetryUtil {

    interface Task<R, T extends Throwable> {
        R perform() throws T;
    }

    
}
