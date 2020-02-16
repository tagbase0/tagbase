package com.oppo.tagbase.job;



/**
 * Created by daikai on 2020/2/16.
 */
public interface Job {


    /**
    *  判断当前job是否成功执行
    *
    */
    boolean succeed(String jobId);


    /**
     *  对外提供一个标签的build接口
     *
     * @param:  标签hive数据的dbName和tableName
     * @return: 返回这个构建job的jobId
     */
    String build(String dbName, String tableName);

    /**
     *  对外提供一个反向字典的build接口
     *
     * @param:  字典所在Hive数据源的dbName和tableName
     * @return: 返回这个构建job的jobId
     */
    String buildDict(String dbName, String tableName);
}
