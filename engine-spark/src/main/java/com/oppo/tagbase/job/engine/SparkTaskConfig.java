package com.oppo.tagbase.job.engine;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.oppo.tagbase.common.guice.Config;

/**
 * Created by liangjingya on 2020/2/20.
 */
@Config("tagbase.job.spark.default")
public class SparkTaskConfig {

    @JsonProperty("driverMemory")
    private String driverMemory;

    @JsonProperty("executorMemory")
    private String executorMemory;

    @JsonProperty("executorInstances")
    private String executorInstances;

    @JsonProperty("memoryOverhead")
    private String memoryOverhead;

    @JsonProperty("queue")
    private String queue;

    @JsonProperty("executorCores")
    private String executorCores;

    @JsonProperty("jarPath")
    private String jarPath;

    @JsonProperty("master")
    private String master;

    @JsonProperty("deployMode")
    private String deployMode;

    @JsonProperty("trackUrl")
    private String trackUrl;

    @JsonProperty("user")
    private String user;

    @JsonProperty("shardItems")
    private int shardItems;

    @JsonProperty("parallelism")
    private int parallelism;

    @JsonProperty("sparkClientErrorLog")
    private String sparkClientErrorLog;

    @JsonProperty("logVerbose")
    private boolean logVerbose;

    public String getDriverMemory() {
        return driverMemory;
    }

    public void setDriverMemory(String driverMemory) {
        this.driverMemory = driverMemory;
    }

    public String getExecutorMemory() {
        return executorMemory;
    }

    public void setExecutorMemory(String executorMemory) {
        this.executorMemory = executorMemory;
    }

    public String getExecutorInstances() {
        return executorInstances;
    }

    public void setExecutorInstances(String executorInstances) {
        this.executorInstances = executorInstances;
    }

    public String getMemoryOverhead() {
        return memoryOverhead;
    }

    public void setMemoryOverhead(String memoryOverhead) {
        this.memoryOverhead = memoryOverhead;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getExecutorCores() {
        return executorCores;
    }

    public void setExecutorCores(String executorCores) {
        this.executorCores = executorCores;
    }

    public String getJarPath() {
        return jarPath;
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getDeployMode() {
        return deployMode;
    }

    public void setDeployMode(String deployMode) {
        this.deployMode = deployMode;
    }

    public String getTrackUrl() {
        return trackUrl;
    }

    public void setTrackUrl(String trackUrl) {
        this.trackUrl = trackUrl;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public int getShardItems() {
        return shardItems;
    }

    public void setShardItems(int shardItems) {
        this.shardItems = shardItems;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public String getSparkClientErrorLog() {
        return sparkClientErrorLog;
    }

    public void setSparkClientErrorLog(String sparkClientErrorLog) {
        this.sparkClientErrorLog = sparkClientErrorLog;
    }

    public boolean isLogVerbose() {
        return logVerbose;
    }

    public void setLogVerbose(boolean logVerbose) {
        this.logVerbose = logVerbose;
    }

    @Override
    public String toString() {
        return "SparkTaskConfig{" +
                "driverMemory='" + driverMemory + '\'' +
                ", executorMemory='" + executorMemory + '\'' +
                ", executorInstances='" + executorInstances + '\'' +
                ", memoryOverhead='" + memoryOverhead + '\'' +
                ", queue='" + queue + '\'' +
                ", executorCores='" + executorCores + '\'' +
                ", jarPath='" + jarPath + '\'' +
                ", master='" + master + '\'' +
                ", deployMode='" + deployMode + '\'' +
                ", trackUrl='" + trackUrl + '\'' +
                ", user='" + user + '\'' +
                ", shardItems=" + shardItems +
                ", parallelism=" + parallelism +
                ", sparkClientErrorLog='" + sparkClientErrorLog + '\'' +
                ", logVerbose=" + logVerbose +
                '}';
    }
}
