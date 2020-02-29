package com.oppo.tagbase.jobv2.spi;

/**
 * Created by liangjingya on 2020/2/20.
 */
public class HiveDictTable {

    private String dbName;//hive库名

    private String tableName;//hive表名

    private String imeiColumnName;//hive表的imei列名

    private String idColumnName;//hive表的id列名

    private String sliceColumnName;//hive表的分区列名

    private int maxId;//目前用户最大id

    public HiveDictTable(String dbName, String tableName, String imeiColumnName, String idColumnName, String sliceColumnName, int maxId) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.imeiColumnName = imeiColumnName;
        this.idColumnName = idColumnName;
        this.sliceColumnName = sliceColumnName;
        this.maxId = maxId;
    }

    public HiveDictTable() {
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getImeiColumnName() {
        return imeiColumnName;
    }

    public void setImeiColumnName(String imeiColumnName) {
        this.imeiColumnName = imeiColumnName;
    }

    public String getIdColumnName() {
        return idColumnName;
    }

    public void setIdColumnName(String idColumnName) {
        this.idColumnName = idColumnName;
    }

    public String getSliceColumnName() {
        return sliceColumnName;
    }

    public void setSliceColumnName(String sliceColumnName) {
        this.sliceColumnName = sliceColumnName;
    }

    public int getMaxId() {
        return maxId;
    }

    public void setMaxId(int maxId) {
        this.maxId = maxId;
    }

    @Override
    public String toString() {
        return "HiveDictTable{" +
                "dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", imeiColumnName='" + imeiColumnName + '\'' +
                ", idColumnName='" + idColumnName + '\'' +
                ", sliceColumnName='" + sliceColumnName + '\'' +
                ", maxId=" + maxId +
                '}';
    }
}
