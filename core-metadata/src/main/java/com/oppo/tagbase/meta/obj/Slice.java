package com.oppo.tagbase.meta.obj;

import java.sql.Date;
import java.util.Objects;

/**
 * Created by wujianchao on 2020/2/5.
 */
public class Slice {

    private long id;
    private Date startTime;
    private Date endTime;
    private long tableId;
    private String sink;
    private SliceStatus status = SliceStatus.BUILDING;
    private int shardNum = 1;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public String getSink() {
        return sink;
    }

    public void setSink(String sink) {
        this.sink = sink;
    }

    public SliceStatus getStatus() {
        return status;
    }

    public void setStatus(SliceStatus status) {
        this.status = status;
    }

    public int getShardNum() {
        return shardNum;
    }

    public void setShardNum(int shardNum) {
        this.shardNum = shardNum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Slice slice = (Slice) o;
        return tableId == slice.getTableId() &&
                Objects.equals(startTime, slice.startTime) &&
                Objects.equals(endTime, slice.endTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, startTime, endTime);
    }

}
