package com.oppo.tagbase.job_v2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.oppo.tagbase.common.guice.Config;

/**
 * Created by wujianchao on 2020/2/26.
 */
@Config("tagbase.job.dict.inverted")
public class InvertedDictConfig {

    @JsonProperty("path")
    private String path;

    public String getPath() {
        return path;
    }
}
