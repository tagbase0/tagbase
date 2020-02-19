package com.oppo.tagbase.storage.core.obj;

/**
 * Created by liangjingya on 2020/2/19.
 */
public class SliceSegment {

    private String sliceDate;//slice对应的日期

    private int segmentId = 1;//bitmap切分后，每个segment对应的分片号，备用

    private String tableName;//hbase表

    private int totalShard = 1;//总分片数

    public SliceSegment(String sliceDate, String tableName, int totalShard) {
        this.sliceDate = sliceDate;
        this.tableName = tableName;
        this.totalShard = totalShard;
    }

    public String getSliceDate() {
        return sliceDate;
    }

    public void setSliceDate(String sliceDate) {
        this.sliceDate = sliceDate;
    }

    public int getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(int segmentId) {
        this.segmentId = segmentId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getTotalShard() {
        return totalShard;
    }

    public void setTotalShard(int totalShard) {
        this.totalShard = totalShard;
    }
}
