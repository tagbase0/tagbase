package com.oppo.tagbase.storage.core.obj;

import java.util.List;

public class StorageQueryContext {

    List<DimensionContext> dimensionContextList;

    private SliceSegment sliceSegment;

    public StorageQueryContext(List<DimensionContext> dimensionContextList, SliceSegment sliceSegment) {
        this.dimensionContextList = dimensionContextList;
        this.sliceSegment = sliceSegment;
    }

    public List<DimensionContext> getDimensionContextList() {
        return dimensionContextList;
    }

    public void setDimensionContextList(List<DimensionContext> dimensionContextList) {
        this.dimensionContextList = dimensionContextList;
    }

    public SliceSegment getSliceSegment() {
        return sliceSegment;
    }

    public void setSliceSegment(SliceSegment sliceSegment) {
        this.sliceSegment = sliceSegment;
    }
}
