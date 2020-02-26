package com.oppo.tagbase.job_v2;

/**
 * Created by wujianchao on 2020/2/26.
 */
public interface Executor<T> {

    void perform(T task) throws Exception;
}
