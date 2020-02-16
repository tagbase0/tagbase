package com.oppo.tagbase.query.node;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author huangfeng
 * @date 2020/2/16 11:35
 */
public enum OutputType {
    @JsonProperty("bitmap")
    BITMAP,
    @JsonProperty("count")
    COUNT

}
