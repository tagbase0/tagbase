package com.oppo.tagbase.jobv2;

/**
 * Created by wujianchao on 2020/3/4.
 */
public class JobPreconditions {

    public static void checkState(boolean expression) {
        if (!expression) {
            throw new JobStateException("");
        }
    }

    public static void checkState(boolean expression, String errorMessage) {
        if (!expression) {
            throw new JobStateException(errorMessage);
        }
    }
}
