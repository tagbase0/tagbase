package com.oppo.tagbase.jobv2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.oppo.tagbase.common.guice.Config;

/**
 * Created by wujianchao on 2020/2/26.
 */
@Config("tagbase.job")
public class JobConfig {

    @JsonProperty("work.dir")
    private String workDir;

    public String getWorkDir() {
        return workDir;
    }

    //TODO add getters
}
