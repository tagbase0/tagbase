package com.oppo.tagbase.storage.core.obj;

import com.google.common.collect.RangeSet;

/**
 * Created by liangjingya on 2020/2/19.
 */
public class InQuery implements Query {

    private String columnName;

    private QueryType type = QueryType.IN_QUERY;

    private RangeSet<String> columnRange;

    public InQuery(String columnName, RangeSet<String> columnRange) {
        this.columnName = columnName;
        this.columnRange = columnRange;
    }

    public RangeSet<String> getColumnRange() {
        return columnRange;
    }

    public void setColumnRange(RangeSet<String> columnRange) {
        this.columnRange = columnRange;
    }

    @Override
    public String getColumn() {
        return columnName;
    }

    @Override
    public QueryType getType() {
        return type;
    }
}
