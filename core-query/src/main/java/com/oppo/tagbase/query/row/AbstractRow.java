package com.oppo.tagbase.query.row;

/**
 * Created by huangfeng on 2020/2/14.
 */
public abstract class AbstractRow implements Row {
    protected Dimensions dims;
    protected String id;

    public void setId(String id) {
        this.id = id;
    }
    public Dimensions getDim() {
        return dims;
    }

}
