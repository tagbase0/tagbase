package com.oppo.tagbase.utils;

/**
 * Created by wujianchao on 2020/2/10.
 */
public class RetryUtil {

    interface Action<R, T extends Throwable> {
        R doAction() throws T;
    }

    
}
