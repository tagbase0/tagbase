package com.oppo.tagbase.jobv2;

/**
 * Created by wujianchao on 2020/2/26.
 */
public interface Executor<T> {

    void perform(T task) throws Exception;
}
