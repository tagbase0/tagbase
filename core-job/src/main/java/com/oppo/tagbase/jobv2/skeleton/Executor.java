package com.oppo.tagbase.jobv2.skeleton;

/**
 * Created by wujianchao on 2020/2/26.
 */
public interface Executor<T> {

    void submit(T task) throws Exception;
}
