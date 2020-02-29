package com.oppo.tagbase.jobv2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.oppo.tagbase.common.guice.Config;

/**
 * Created by wujianchao on 2020/2/29.
 */
@Config("tagbase.job.scheduler.singleton")
public class SingletonSchedulerConfig {

    @JsonProperty(value = "parallelism", defaultValue = "20")
    private int parallelism;

    @JsonProperty(value = "interval", defaultValue = "10")
    private int interval;

    public int getParallelism() {
        return parallelism;
    }

    public int getInterval() {
        return interval;
    }
}
