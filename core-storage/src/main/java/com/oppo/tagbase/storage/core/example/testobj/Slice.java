package com.oppo.tagbase.storage.core.example.testobj;

/**
 * Created by wujianchao on 2020/2/5.
 */
public class Slice {

    private String name;
    private String sink;
    private SliceStatus status;
    private int shardNum = 1;

    public Slice(String name, String sink, SliceStatus status) {
        this.name = name;
        this.sink = sink;
        this.status = status;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
}
