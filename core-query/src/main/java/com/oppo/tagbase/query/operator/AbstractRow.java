package com.oppo.tagbase.query.operator;

/**
 * Created by huangfeng on 2020/2/14.
 */
public abstract class AbstractRow implements Row{
    String sourceId;
    protected Dimensions dims;

    public Dimensions getDim() {
        return dims;
    }


}
