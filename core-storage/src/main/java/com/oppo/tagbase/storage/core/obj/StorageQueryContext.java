package com.oppo.tagbase.storage.core.obj;

import java.util.List;

public class StorageQueryContext {

    List<DimContext> dimContextList;

    private SliceSegment sliceSegment;

    private String queryId;

    public StorageQueryContext(List<DimContext> dimContextList, SliceSegment sliceSegment, String queryId) {
        this.dimContextList = dimContextList;
        this.sliceSegment = sliceSegment;
        this.queryId = queryId;
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

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    @Override
    public String toString() {
        return "StorageQueryContext{" +
                "dimContextList=" + dimContextList +
                ", sliceSegment=" + sliceSegment +
                ", queryId='" + queryId + '\'' +
                '}';
    }
}
