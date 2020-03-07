package com.oppo.tagbase.storage.core.connector;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.oppo.tagbase.common.guice.Config;

/**
 * Created by liangjingya on 2020/2/8.
 */
@Config("tagbase.storage.core")
public class StorageConnectorConfig {

    @JsonProperty("queryPoolMaxThread")
    private int queryPoolMaxThread = 10;

    @JsonProperty("queryPoolCoreThread")
    private int queryPoolCoreThread = 5;

    @JsonProperty("queryPoolKeepAliveSecond")
    private int queryPoolKeepAliveSecond = 300;

    @JsonProperty("queryPoolQueueSie")
    private int queryPoolQueueSie = 500;

    public int getQueryPoolMaxThread() {
        return queryPoolMaxThread;
    }

    public void setQueryPoolMaxThread(int queryPoolMaxThread) {
        this.queryPoolMaxThread = queryPoolMaxThread;
    }

    public int getQueryPoolCoreThread() {
        return queryPoolCoreThread;
    }

    public void setQueryPoolCoreThread(int queryPoolCoreThread) {
        this.queryPoolCoreThread = queryPoolCoreThread;
    }

    public int getQueryPoolKeepAliveSecond() {
        return queryPoolKeepAliveSecond;
    }

    public void setQueryPoolKeepAliveSecond(int queryPoolKeepAliveSecond) {
        this.queryPoolKeepAliveSecond = queryPoolKeepAliveSecond;
    }

    public int getQueryPoolQueueSie() {
        return queryPoolQueueSie;
    }

    public void setQueryPoolQueueSie(int queryPoolQueueSie) {
        this.queryPoolQueueSie = queryPoolQueueSie;
    }

    @Override
    public String toString() {
        return "StorageConnectorConfig{" +
                "queryPoolMaxThread=" + queryPoolMaxThread +
                ", queryPoolCoreThread=" + queryPoolCoreThread +
                ", queryPoolKeepAliveSecond=" + queryPoolKeepAliveSecond +
                ", queryPoolQueueSie=" + queryPoolQueueSie +
                '}';
    }
}
