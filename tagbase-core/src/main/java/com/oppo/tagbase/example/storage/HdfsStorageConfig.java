package com.oppo.tagbase.example.storage;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

/**
 * Created by wujianchao on 2020/1/17.
 */
public class HdfsStorageConfig {

    @JsonProperty
    @NotNull
    private String path;

    public String getPath(){
        return path;
    }

}
