package com.oppo.tagbase.storage.core.obj;

import com.google.common.collect.RangeSet;

/**
 * Created by liangjingya on 2020/2/15.
 */
public class ColumnDomain<T extends Comparable> {

    private RangeSet<T> columnRange;

    private String columnName;

    public ColumnDomain(RangeSet<T> sliceRange, String sliceName) {
        this.columnRange = sliceRange;
        this.columnName = sliceName;
    }

    public RangeSet<T> getColumnRange() {
        return columnRange;
    }

    public void setColumnRange(RangeSet<T> columnRange) {
        this.columnRange = columnRange;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
}
