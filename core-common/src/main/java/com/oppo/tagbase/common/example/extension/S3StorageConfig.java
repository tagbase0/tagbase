package com.oppo.tagbase.common.example.extension;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

/**
 * Created by wujianchao on 2020/2/2.
 */
public class S3StorageConfig {

    @JsonProperty
    @NotNull
    private String bucket;

    public String getBucket() {
        return bucket;
    }
}
