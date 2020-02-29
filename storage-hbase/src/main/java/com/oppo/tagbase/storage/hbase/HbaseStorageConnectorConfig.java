package com.oppo.tagbase.storage.hbase;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
    private String tablePrefix = "tagbase_";

    @JsonProperty
    private String family = "f1";

    @JsonProperty
    private String qualifier = "q1";

    @JsonIgnore
    private String rowkeyDelimiter = "\u0001";

    @JsonProperty
    private int scanCacheSize = 100;

    @JsonProperty
    private int scanMaxResultSize = 5 * 1024 * 1024;

    public String getTablePrefix() {
        return tablePrefix;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
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
                ", tablePrefix='" + tablePrefix + '\'' +
                ", family='" + family + '\'' +
                ", qualifier='" + qualifier + '\'' +
                ", rowkeyDelimiter='" + rowkeyDelimiter + '\'' +
                ", scanCacheSize=" + scanCacheSize +
                ", scanMaxResultSize=" + scanMaxResultSize +
                '}';
    }
}
