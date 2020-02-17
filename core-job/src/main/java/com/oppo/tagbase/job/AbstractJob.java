package com.oppo.tagbase.job;

import com.oppo.tagbase.meta.obj.Job;

import java.util.concurrent.*;

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
     * 判断当前job是否成功执行
     */
    boolean succeed(String jobId);


    /**
     * 对外提供一个标签的build接口
     *
     * @param: 标签hive数据的dbName和tableName，以及此job的类型
     * @return: 返回这个构建job的jobId
     */
    String build(String dbName, String tableName, String jobType);

    /**
     * 初始化 job, 完成元数据模块的相关创建工作
     */
    void iniJob(Job job);

}
