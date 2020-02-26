package com.oppo.tagbase.job_v2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.oppo.tagbase.common.guice.Config;

/**
 * Created by wujianchao on 2020/2/26.
 */
@Config("tagbase.job")
public class JobConfig {

    @JsonProperty("work.dir")
    private String workDir;

    @JsonProperty("running.max")
    private int maxRunning;

    public String getWorkDir() {
        return workDir;
    }

    //TODO add getters
}
