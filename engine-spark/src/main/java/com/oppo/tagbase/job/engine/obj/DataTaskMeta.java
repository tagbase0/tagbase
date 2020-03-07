package com.oppo.tagbase.job.engine.obj;

import java.util.List;

/**
 * Created by liangjingya on 2020/3/05.
 */
public class DataTaskMeta {

    private String dictBasePath;//反向字典表的hdfspath

    private int maxRowPartition;//每个生成的hfile文件存放做大行数，用于spark分区

    private String outputPath;//hfile文件输出位置

    private String dbName;//hive库名

    private String tableName;//hive表名

    private List<String> dimColumnNames;//hive表维度的列名和索引，例如app，event，version

    private String imeiColumnName;//hive表的imei列名

    private String sliceColumnName;//需要处理的分区列名

    private String sliceColumnnValueLeft;//分区下界，左闭

    private String sliceColumnValueRight;//分区上界，右开

    public DataTaskMeta(String dictBasePath, int maxRowPartition, String outputPath, String dbName, String tableName, List<String> dimColumnNames, String imeiColumnName, String sliceColumnName, String sliceColumnnValueLeft, String sliceColumnValueRight) {
        this.dictBasePath = dictBasePath;
        this.maxRowPartition = maxRowPartition;
        this.outputPath = outputPath;
        this.dbName = dbName;
        this.tableName = tableName;
        this.dimColumnNames = dimColumnNames;
        this.imeiColumnName = imeiColumnName;
        this.sliceColumnName = sliceColumnName;
        this.sliceColumnnValueLeft = sliceColumnnValueLeft;
        this.sliceColumnValueRight = sliceColumnValueRight;
    }

    public DataTaskMeta() {
    }

    public String getDictBasePath() {
        return dictBasePath;
    }

    public void setDictBasePath(String dictBasePath) {
        this.dictBasePath = dictBasePath;
    }

    public int getMaxRowPartition() {
        return maxRowPartition;
    }

    public void setMaxRowPartition(int maxRowPartition) {
        this.maxRowPartition = maxRowPartition;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
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

    public List<String> getDimColumnNames() {
        return dimColumnNames;
    }

    public void setDimColumnNames(List<String> dimColumnNames) {
        this.dimColumnNames = dimColumnNames;
    }

    public String getImeiColumnName() {
        return imeiColumnName;
    }

    public void setImeiColumnName(String imeiColumnName) {
        this.imeiColumnName = imeiColumnName;
    }

    public String getSliceColumnName() {
        return sliceColumnName;
    }

    public void setSliceColumnName(String sliceColumnName) {
        this.sliceColumnName = sliceColumnName;
    }

    public String getSliceColumnnValueLeft() {
        return sliceColumnnValueLeft;
    }

    public void setSliceColumnnValueLeft(String sliceColumnnValueLeft) {
        this.sliceColumnnValueLeft = sliceColumnnValueLeft;
    }

    public String getSliceColumnValueRight() {
        return sliceColumnValueRight;
    }

    public void setSliceColumnValueRight(String sliceColumnValueRight) {
        this.sliceColumnValueRight = sliceColumnValueRight;
    }

    @Override
    public String toString() {
        return "DictTaskMeta{" +
                "dictBasePath='" + dictBasePath + '\'' +
                ", maxRowPartition=" + maxRowPartition +
                ", outputPath='" + outputPath + '\'' +
                ", dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", dimColumnNames=" + dimColumnNames +
                ", imeiColumnName='" + imeiColumnName + '\'' +
                ", sliceColumnName='" + sliceColumnName + '\'' +
                ", sliceColumnnValueLeft='" + sliceColumnnValueLeft + '\'' +
                ", sliceColumnValueRight='" + sliceColumnValueRight + '\'' +
                '}';
    }
}
