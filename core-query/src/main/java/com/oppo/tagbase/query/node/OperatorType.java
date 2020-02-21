package com.oppo.tagbase.query.node;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author huangfeng
 * @date 2020/2/16 11:55
 */
public enum OperatorType {
    @JsonProperty("intersect")
    INTERSECT,
    @JsonProperty("union")
    UNION,
    @JsonProperty("diff")
    DIFF
}
