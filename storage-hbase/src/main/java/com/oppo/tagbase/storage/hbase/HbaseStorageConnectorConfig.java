package com.oppo.tagbase.storage.hbase;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

/**
 * Created by liangjingya on 2020/2/8.
 */
public class HbaseStorageConnectorConfig {
    @JsonProperty
    private String zkPort = "2181";

    @JsonProperty
    @NotNull
    private String zkQuorum;

    @JsonProperty
    @NotNull
    private String rootDir;

    @JsonProperty
    private String nameSpace = "tagbase";

    @JsonProperty
    private String family = "f1";

    @JsonProperty
    private String qualifier = "q1";

    @JsonProperty
    private String rowkeyDelimiter = "_";

    @JsonProperty
    private int threadPoolmaxThread = 10;

    @JsonProperty
    private int threadPoolcoreThread = 5;

    @JsonProperty
    private int threadPoolkeepAliveSecond = 300;

    @JsonProperty
    private int threadPoolQueueSize = 500;

    @JsonProperty
    private int scanCacheSize = 100;

    @JsonProperty
    private int scanMaxResultSize = 5 * 1024 * 1024;

    public int getThreadPoolQueueSize() {
        return threadPoolQueueSize;
    }

    public void setThreadPoolQueueSize(int threadPoolQueueSize) {
        this.threadPoolQueueSize = threadPoolQueueSize;
    }

    public String getZkPort() {
        return zkPort;
    }

    public void setZkPort(String zkPort) {
        this.zkPort = zkPort;
    }

    public String getZkQuorum() {
        return zkQuorum;
    }

    public void setZkQuorum(String zkQuorum) {
        this.zkQuorum = zkQuorum;
    }

    public String getRootDir() {
        return rootDir;
    }

    public void setRootDir(String rootDir) {
        this.rootDir = rootDir;
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public String getRowkeyDelimiter() {
        return rowkeyDelimiter;
    }

    public void setRowkeyDelimiter(String rowkeyDelimiter) {
        this.rowkeyDelimiter = rowkeyDelimiter;
    }

    public int getThreadPoolmaxThread() {
        return threadPoolmaxThread;
    }

    public void setThreadPoolmaxThread(int threadPoolmaxThread) {
        this.threadPoolmaxThread = threadPoolmaxThread;
    }

    public int getThreadPoolcoreThread() {
        return threadPoolcoreThread;
    }

    public void setThreadPoolcoreThread(int threadPoolcoreThread) {
        this.threadPoolcoreThread = threadPoolcoreThread;
    }

    public int getThreadPoolkeepAliveSecond() {
        return threadPoolkeepAliveSecond;
    }

    public void setThreadPoolkeepAliveSecond(int threadPoolkeepAliveSecond) {
        this.threadPoolkeepAliveSecond = threadPoolkeepAliveSecond;
    }

    public int getScanCacheSize() {
        return scanCacheSize;
    }

    public void setScanCacheSize(int scanCacheSize) {
        this.scanCacheSize = scanCacheSize;
    }

    public int getScanMaxResultSize() {
        return scanMaxResultSize;
    }

    public void setScanMaxResultSize(int scanMaxResultSize) {
        this.scanMaxResultSize = scanMaxResultSize;
    }

    @Override
    public String toString() {
        return "HbaseStorageConnectorConfig{" +
                "zkPort='" + zkPort + '\'' +
                ", zkQuorum='" + zkQuorum + '\'' +
                ", rootDir='" + rootDir + '\'' +
                ", nameSpace='" + nameSpace + '\'' +
                ", family='" + family + '\'' +
                ", qualifier='" + qualifier + '\'' +
                ", rowkeyDelimiter='" + rowkeyDelimiter + '\'' +
                ", threadPoolmaxThread=" + threadPoolmaxThread +
                ", threadPoolcoreThread=" + threadPoolcoreThread +
                ", threadPoolkeepAliveSecond=" + threadPoolkeepAliveSecond +
                ", scanCacheSize=" + scanCacheSize +
                ", scanMaxResultSize=" + scanMaxResultSize +
                '}';
    }
}
