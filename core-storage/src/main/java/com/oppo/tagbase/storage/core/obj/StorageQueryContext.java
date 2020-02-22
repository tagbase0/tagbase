package com.oppo.tagbase.storage.core.obj;

import java.util.List;

public class StorageQueryContext {

    List<DimContext> dimContextList;

    private SliceSegment sliceSegment;

    public StorageQueryContext(List<DimContext> dimContextList, SliceSegment sliceSegment) {
        this.dimContextList = dimContextList;
        this.sliceSegment = sliceSegment;
    }

    public List<DimContext> getDimContextList() {
        return dimContextList;
    }

    public void setDimContextList(List<DimContext> dimContextList) {
        this.dimContextList = dimContextList;
    }

    public SliceSegment getSliceSegment() {
        return sliceSegment;
    }

    public void setSliceSegment(SliceSegment sliceSegment) {
        this.sliceSegment = sliceSegment;
    }

    @Override
    public String toString() {
        return "StorageQueryContext{" +
                "dimContextList=" + dimContextList +
                ", sliceSegment=" + sliceSegment +
                '}';
    }
}
