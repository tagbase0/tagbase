package com.oppo.tagbase.meta.obj;

import java.util.Objects;

/**
 * Created by wujianchao on 2020/2/5.
 */
public class Slice {

    private long id;
    private String name;
    private long tableId;
    private String sink;
    private SliceStatus status;
    private int shardNum = 1;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
        return tableId == slice.tableId &&
                Objects.equals(name, slice.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, tableId);
    }

}
