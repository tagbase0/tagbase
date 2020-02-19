package com.oppo.tagbase.storage.core.obj;

import com.google.common.collect.RangeSet;
import java.sql.Date;

/**
 * Created by liangjingya on 2020/2/15.
 */
public class SliceQuery implements Query {

    private RangeSet<Date> sliceRange;

    private String sliceName;

    private QueryType type = QueryType.SLICE_QUERY;

    public SliceQuery(RangeSet<Date> sliceRange, String sliceName) {
        this.sliceRange = sliceRange;
        this.sliceName = sliceName;
    }

    public RangeSet<Date> getSliceRange() {
        return sliceRange;
    }

    public void setSliceRange(RangeSet<Date> sliceRange) {
        this.sliceRange = sliceRange;
    }

    @Override
    public String getColumn() {
        return sliceName;
    }

    @Override
    public QueryType getType() {
        return type;
    }
}
