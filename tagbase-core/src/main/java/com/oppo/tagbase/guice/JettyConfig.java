package com.oppo.tagbase.guice;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;

/**
 * Created by wujianchao on 2020/2/2.
 */
public class JettyConfig {

    private int port = 2020;

    @Min(1)
    @JsonProperty("threads")
    private int numThreads = 50;

    @Min(1000)
    @JsonProperty("timeout")
    private long maxQueryTimeout = 10_000;

    public int getPort() {
        return port;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public long getMaxQueryTimeout() {
        return maxQueryTimeout;
    }
}
