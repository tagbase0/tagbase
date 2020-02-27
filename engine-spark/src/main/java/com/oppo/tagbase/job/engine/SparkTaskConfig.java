package com.oppo.tagbase.job.engine;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by liangjingya on 2020/2/20.
 */
public class SparkTaskConfig {

    @JsonProperty
    private String driverMemory;

    @JsonProperty
    private String executorMemory;

    @JsonProperty
    private String executorInstances;

    @JsonProperty
    private String queue;

    @JsonProperty
    private String executorCores;

    @JsonProperty
    private String parallelism;

    @JsonProperty
    private String mainClass;

    @JsonProperty
    private String jarPath;

    @JsonProperty
    private String master;

    @JsonProperty
    private String deployMode;

    @JsonProperty
    private String trackUrl;

    @JsonProperty
    private String user;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getTrackUrl() {
        return trackUrl;
    }

    public void setTrackUrl(String trackUrl) {
        this.trackUrl = trackUrl;
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

    public String getMainClass() {
        return mainClass;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }

    public String getJarPath() {
        return jarPath;
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }

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

    public String getParallelism() {
        return parallelism;
    }

    public void setParallelism(String parallelism) {
        this.parallelism = parallelism;
    }

    @Override
    public String toString() {
        return "SparkJobConfig{" +
                "driverMemory='" + driverMemory + '\'' +
                ", executorMemory='" + executorMemory + '\'' +
                ", executorInstances='" + executorInstances + '\'' +
                ", queue='" + queue + '\'' +
                ", executorCores='" + executorCores + '\'' +
                ", parallelism='" + parallelism + '\'' +
                ", mainClass='" + mainClass + '\'' +
                ", jarPath='" + jarPath + '\'' +
                ", master='" + master + '\'' +
                ", deployMode='" + deployMode + '\'' +
                ", trackUrl='" + trackUrl + '\'' +
                ", user='" + user + '\'' +
                '}';
    }
}
