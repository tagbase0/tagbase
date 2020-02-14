package com.oppo.tagbase.query.operator;

/**
 * Created by huangfeng on 2020/2/14.
 */
public class ResultRow  extends  AbstractRow{
    int metric;


    public ResultRow(Dimensions dims, int cardinality) {
        super();
        this.dims = dims;
        this.metric = cardinality;

    }
}
