package com.oppo.tagbase.dict.util;

/**
 * Created by wujianchao on 2020/2/10.
 */
// TODO implement
public class RetryUtil {

    interface Task<R, T extends Throwable> {
        R perform() throws T;
    }

    
}
