package com.oppo.tagbase.query.row;

import com.oppo.tagbase.storage.core.obj.AbstractRow;
import com.oppo.tagbase.storage.core.obj.Dimensions;

/**
 * Created by huangfeng on 2020/2/14.
 */
public class ResultRow<T> extends AbstractRow {
    String id;
    T metric;

    public ResultRow(Dimensions dims, T cardinality) {
        super(dims);
        this.metric = cardinality;
    }

    public ResultRow(String id, Dimensions dims, T cardinality) {
        super(dims);
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
