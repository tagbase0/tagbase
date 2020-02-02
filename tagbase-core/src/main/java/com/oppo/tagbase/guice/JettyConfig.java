package com.oppo.tagbase.guice;

/**
 * Created by wujianchao on 2020/2/2.
 */
public class JettyConfig {

    private int port = 2020;

    private int numThreads = 50;

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
