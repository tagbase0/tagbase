package com.oppo.tagbase.job.engine.obj;

import java.util.List;

/**
 * Created by liangjingya on 2020/2/20.
 */
public class HiveSrcTable {

    private String dbName;//hive库名

    private String tableName;//hive表名

    private List<String> dimColumns;//hive表维度的列名和索引，例如app，event，version

    private SliceColumn sliceColumn;//需要处理的分区信息

    private String imeiColumnName;//hive表的imei列名

    public HiveSrcTable(String dbName, String tableName, List<String> dimColumns, SliceColumn sliceColumn, String imeiColumnName) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.dimColumns = dimColumns;
        this.sliceColumn = sliceColumn;
        this.imeiColumnName = imeiColumnName;
    }

    public HiveSrcTable() {
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public List<String> getDimColumns() {
        return dimColumns;
    }

    public void setDimColumns(List<String> dimColumns) {
        this.dimColumns = dimColumns;
    }

    public SliceColumn getSliceColumn() {
        return sliceColumn;
    }

    public void setSliceColumn(SliceColumn sliceColumn) {
        this.sliceColumn = sliceColumn;
    }

    public String getImeiColumnName() {
        return imeiColumnName;
    }

    public void setImeiColumnName(String imeiColumnName) {
        this.imeiColumnName = imeiColumnName;
    }

    @Override
    public String toString() {
        return "HiveSrcTable{" +
                "dbName='" + dbName + '\'' +
                ", dimColumns=" + dimColumns +
                ", sliceColumn=" + sliceColumn +
                ", imeiColumnName='" + imeiColumnName + '\'' +
                '}';
    }
}
