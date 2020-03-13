package com.oppo.tagbase.job.obj;

/**
 * Created by liangjingya on 2020/3/05.
 */
public class DictTaskMeta {

    private String dictBasePath;//反向字典表的hdfspath

    private long maxId;//当前最大用户id

    private int maxRowPartition;//每个生成的字典文件存放做大行数，用于spark分区

    private String outputPath;//反向字典任务输出位置

    private String dbName;//hive库名

    private String tableName;//hive表名

    private String imeiColumnName;//hive表的imei列名

    private String sliceColumnName;//需要处理的分区信息

    private String sliceColumnnValueLeft;

    private String sliceColumnValueRight;

    public DictTaskMeta(String dictBasePath, long maxId, int maxRowPartition, String outputPath, String dbName, String tableName, String imeiColumnName, String sliceColumnName, String sliceColumnnValueLeft, String sliceColumnValueRight) {
        this.dictBasePath = dictBasePath;
        this.maxId = maxId;
        this.maxRowPartition = maxRowPartition;
        this.outputPath = outputPath;
        this.dbName = dbName;
        this.tableName = tableName;
        this.imeiColumnName = imeiColumnName;
        this.sliceColumnName = sliceColumnName;
        this.sliceColumnnValueLeft = sliceColumnnValueLeft;
        this.sliceColumnValueRight = sliceColumnValueRight;
    }

    public DictTaskMeta() {
    }

    public String getDictBasePath() {
        return dictBasePath;
    }

    public void setDictBasePath(String dictBasePath) {
        this.dictBasePath = dictBasePath;
    }

    public long getMaxId() {
        return maxId;
    }

    public void setMaxId(long maxId) {
        this.maxId = maxId;
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
                ", maxId=" + maxId +
                ", maxRowPartition=" + maxRowPartition +
                ", outputPath='" + outputPath + '\'' +
                ", dbName='" + dbName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", imeiColumnName='" + imeiColumnName + '\'' +
                ", sliceColumnName='" + sliceColumnName + '\'' +
                ", sliceColumnnValueLeft='" + sliceColumnnValueLeft + '\'' +
                ", sliceColumnValueRight='" + sliceColumnValueRight + '\'' +
                '}';
    }
}
