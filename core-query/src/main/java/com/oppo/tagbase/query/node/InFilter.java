package com.oppo.tagbase.query.node;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Created by huangfeng on 2020/2/15.
 */
public class InFilter implements Filter {

    private String column;
    private final SortedSet<String> values;

    @JsonCreator
    public InFilter(
            @JsonProperty("column") String column,
            @JsonProperty("values") Collection<String> values
    ) {
        Preconditions.checkNotNull(column, "column can not be null");
        Preconditions.checkArgument(values != null, "values can not be null");

        this.values = new TreeSet<>();
        for (String value : values) {
            this.values.add(value);
        }
        this.column = column;

    }


    @Override
    public String getColumn() {
        return column;
    }

    @Override
    public boolean isExact() {
        return values.size() == 1;
    }

    @Override
    public RangeSet<String> getDimensionRangeSet() {

        RangeSet<String> retSet = TreeRangeSet.create();
        for (String value : values) {
            retSet.add(Range.singleton(value));
        }
        return retSet;
    }

}
