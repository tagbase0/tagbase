package com.oppo.tagbase.query.node;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * @author huangfeng
 * @date 2020/2/26 14:59
 */
public abstract  class BaseQuery implements Query{
    @JsonIgnore
    String id;


    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getId() {
        return id;
    }
}
