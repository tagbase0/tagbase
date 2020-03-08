package com.oppo.tagbase.job.engine;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by liangjingya on 2020/3/07.
 */
public class SparkConfigConstant {

    public static final String SPARK_MASTER = "spark.master";
    public static final String DEPLOY_MODE = "spark.submit.deployMode";

    public static final String DRIVER_MEMORY = "spark.driver.memory";
    public static final String EXECUTOR_MEMORY = "spark.executor.memory";
    public static final String EXECUTOR_CORES = "spark.executor.cores";
    public static final String EXECUTOR_INSTANCES = "spark.executor.instances";

    public static final String DEFAULT_PARALLELISM = "spark.default.parallelism";
    public static final String SQL_SHUFFLE_PARTITION = "spark.sql.shuffle.partitions";
    public static final String YARN_QUEUE = "spark.yarn.queue";
    public static final String EXECUTOR_MEMORY_OVERHEAD = "spark.executor.memoryOverhead";

    public static final String WAIT_APP_COMPLETION = "spark.yarn.submit.waitAppCompletion";
    public static final String MAX_APP_ATTEMPTS = "spark.yarn.maxAppAttempts";

    public static final String DRIVER_EXTRA_CLASSPATH = "spark.driver.extraClassPath";
    public static final String DRIVER_EXTRA_JAVA_OPTIONS = "spark.driver.extraJavaOptions";
    public static final String DRIVER_EXTRA_LIBRARY_PATH = "spark.driver.extraLibraryPath";
    public static final String EXECUTOR_EXTRA_CLASSPATH = "spark.executor.extraClassPath";
    public static final String EXECUTOR_EXTRA_JAVA_OPTIONS = "spark.executor.extraJavaOptions";
    public static final String EXECUTOR_EXTRA_LIBRARY_PATH = "spark.executor.extraLibraryPath";

    public static final Set<String> USER_CONFIG_WHITELIST = new HashSet<String>(){
        {
            add(EXECUTOR_MEMORY);
            add(EXECUTOR_INSTANCES);
            add(EXECUTOR_CORES);
            add(YARN_QUEUE);
            add(EXECUTOR_MEMORY_OVERHEAD);
        }
    };


}
