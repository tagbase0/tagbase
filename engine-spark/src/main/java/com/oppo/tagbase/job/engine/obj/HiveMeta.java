package com.oppo.tagbase.job.engine.obj;

/**
 * Created by liangjingya on 2020/2/20.
 */
public class HiveMeta {

    private String dictTablePath;//反向字典表的hdfspath

    private int maxId;//目前最大用户id

    private HiveSrcTable hiveSrcTable;//tag、event等hive表

    private String outputPath;//输出位置，反向字典任务为分区目录，bigmap任务为hfile的目录

    private String rowCountPath;//统计文件行数文件的hdfspath

    public HiveMeta(String dictTablePath, int maxId, HiveSrcTable hiveSrcTable, String outputPath, String rowCountPath) {
        this.dictTablePath = dictTablePath;
        this.maxId = maxId;
        this.hiveSrcTable = hiveSrcTable;
        this.outputPath = outputPath;
        this.rowCountPath = rowCountPath;
    }

    public HiveMeta() {
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public String getDictTablePath() {
        return dictTablePath;
    }

    public void setDictTablePath(String dictTablePath) {
        this.dictTablePath = dictTablePath;
    }

    public HiveSrcTable getHiveSrcTable() {
        return hiveSrcTable;
    }

    public void setHiveSrcTable(HiveSrcTable hiveSrcTable) {
        this.hiveSrcTable = hiveSrcTable;
    }

    public String getRowCountPath() {
        return rowCountPath;
    }

    public void setRowCountPath(String rowCountPath) {
        this.rowCountPath = rowCountPath;
    }

    public int getMaxId() {
        return maxId;
    }

    public void setMaxId(int maxId) {
        this.maxId = maxId;
    }

    @Override
    public String toString() {
        return "HiveMeta{" +
                "dictTablePath='" + dictTablePath + '\'' +
                ", maxId='" + maxId + '\'' +
                ", hiveSrcTable=" + hiveSrcTable +
                ", outputPath='" + outputPath + '\'' +
                ", rowCountPath='" + rowCountPath + '\'' +
                '}';
    }
}
