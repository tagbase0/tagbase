package com.oppo.tagbase.storage.core.obj;

public class SlicePartitionMeta {

    private String date;//标志具体的日期

    private int pattition = 1;//查询的分片号

    private String hbaseTable;//hbase表

    private int shardNum = 1;//总分区数

    public SlicePartitionMeta(String hbaseTable, int shardNum, String date) {
        this.shardNum = shardNum;
        this.hbaseTable = hbaseTable;
        this.date = date;
    }

    public int getShardNum() {
        return shardNum;
    }

    public void setShardNum(int shardNum) {
        this.shardNum = shardNum;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getPattition() {
        return pattition;
    }

    public void setPattition(int pattition) {
        this.pattition = pattition;
    }

    public String getHbaseTable() {
        return hbaseTable;
    }

    public void setHbaseTable(String hbaseTable) {
        this.hbaseTable = hbaseTable;
    }
}
