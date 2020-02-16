package com.oppo.tagbase.storage.core.obj;

import java.util.List;

public class ScanMeta {

    List<FilterMeta> filterMetaList;

    private SlicePartitionMeta slicePartition;

    public ScanMeta(List<FilterMeta> filterMetaList, SlicePartitionMeta slicePartition) {
        this.filterMetaList = filterMetaList;
        this.slicePartition = slicePartition;
    }

    public List<FilterMeta> getFilterMetaList() {
        return filterMetaList;
    }

    public void setFilterMetaList(List<FilterMeta> filterMetaList) {
        this.filterMetaList = filterMetaList;
    }

    public SlicePartitionMeta getSlicePartition() {
        return slicePartition;
    }

    public void setSlicePartition(SlicePartitionMeta slicePartition) {
        this.slicePartition = slicePartition;
    }
}
