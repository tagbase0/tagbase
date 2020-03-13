package com.oppo.tagbase.jobv2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.oppo.tagbase.common.guice.Config;

/**
 * Created by wujianchao on 2020/2/26.
 */
@Config("tagbase.job")
public class JobConfig {

    @JsonProperty("work.base.dir")
    private String workDir;

    @JsonProperty("bitmap.dir")
    private String bitmapDir;

    @JsonProperty("dict.inverted.base.path")
    private String invertedDictPath;

    @JsonProperty("dict.forward.base.path")
    private String forwardDictPath;

    @JsonProperty("pending.limit")
    private int pendingLimit;

    public String getWorkDir() {
        return workDir;
    }

    public String getBitmapDir() {
        return bitmapDir;
    }

    public String getInvertedDictPath() {
        return invertedDictPath;
    }

    public String getForwardDictPath() {
        return forwardDictPath;
    }

    public int getPendingLimit() {
        return pendingLimit;
    }
}
