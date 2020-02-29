package com.oppo.tagbase.query;

import com.google.common.collect.RangeSet;
import com.oppo.tagbase.meta.obj.Column;

/**
 * @author huangfeng
 * @date 2020/2/21 13:36
 */
public class FilterAnalysis<T extends  Comparable> {

    private RangeSet<T> columnRange;

    private Column column;

    private int cardinality;


    public FilterAnalysis(Column column, RangeSet<T> columnRange,int cardinality) {
        this.columnRange = columnRange;
        this.column = column;
        this.cardinality = cardinality;
    }

    public RangeSet<T> getColumnRange() {
        return columnRange;
    }

    public void setColumnRange(RangeSet<T> columnRange) {
        this.columnRange = columnRange;
    }


    public int getCardinality() {
        return cardinality;
    }

    public Column getColumn() {
        return column;
    }

    @Override
    public String toString() {
        return "{" +
                "range=" + columnRange +
                ", cardinality=" + cardinality +
                '}';
    }
}
