package com.oppo.tagbase.query.operator;

/**
 * Created by huangfeng on 2020/2/14.
 */
public class ResultRow<T>  extends  AbstractRow{
    String id;
    T metric;
    public ResultRow(Dimensions dims, T cardinality) {
        super();
        this.dims = dims;
        this.metric = cardinality;
    }

    public ResultRow(String id,Dimensions dims, T cardinality) {
        super();
        this.dims = dims;
        this.id = id;
        this.metric = cardinality;
    }

    public String id() {
        return id;
    }

    public T getMetric() {
        return metric;
    }
}
