package com.oppo.tagbase.storage.connector;

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
    private int threadPoolSize = 20;

    public String getZkPort() {
        return zkPort;
    }

    public String getZkQuorum() {
        return zkQuorum;
    }

    public String getRootDir() {
        return rootDir;
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public String getFamily() {
        return family;
    }

    public String getQualifier() {
        return qualifier;
    }

    public String getRowkeyDelimiter() {
        return rowkeyDelimiter;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    @Override
    public String toString() {
        return "hbaseStorageConnectorConfig{" +
                "zkPort='" + zkPort + '\'' +
                ", zkQuorum='" + zkQuorum + '\'' +
                ", rootDir='" + rootDir + '\'' +
                ", nameSpace='" + nameSpace + '\'' +
                ", family='" + family + '\'' +
                ", qualifier='" + qualifier + '\'' +
                ", rowkeyDelimiter='" + rowkeyDelimiter + '\'' +
                ", threadPoolSize=" + threadPoolSize +
                '}';
    }
}
