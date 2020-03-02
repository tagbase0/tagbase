package com.oppo.tagbase.job;

import com.oppo.tagbase.meta.obj.Job;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by daikai on 2020/2/17.
 */
public interface AbstractJob {


    ConcurrentLinkedQueue<Job> PENDING_JOBS_QUEUE = new ConcurrentLinkedQueue<>();
    ConcurrentLinkedQueue<Job> RUNNING_JOBS_QUEUE = new ConcurrentLinkedQueue<>();

    ExecutorService JOB_EXECUTORS = new ThreadPoolExecutor(
            200,
            500,
            60L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1000),
            new ThreadPoolExecutor.AbortPolicy());


    /**
     * 对外提供一个字典的构建接口
     *
     * @param: 标签hive数据的dbName和tableName
     */
    void buildDict(String dbName, String tableName);

    /**
     * 对外提供一个标签的构建接口
     *
     * @param: 标签hive数据的dbName和tableName，以及构建的时间区间
     * @return: 返回这个构建job的jobId
     */
    String buildData(String dbName, String tableName, String lowerDate, String upperDate);

    /**
     * 对外提供一个查询构建job信息的接口
     *
     * @param: 构建任务的 jobId
     * @return: 返回这个构建job的运行状态
     */
    Job jobState(String jobId);


}
