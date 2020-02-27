package com.oppo.tagbase.job.engine.obj;

/**
 * Created by liangjingya on 2020/2/20.
 */
public class HiveMeta {

    private HiveDictTable hiveDictTable;//反向字典表

    private HiveSrcTable hiveSrcTable;//维度表

    private String output;//输出位置，反向字典任务为分区号，bigmap任务为hdfspath

    private String rowCountPath;//统计文件行数文件的hdfspath

    public HiveMeta(HiveDictTable hiveDictTable, HiveSrcTable hiveSrcTable, String output, String rowCountPath) {
        this.hiveDictTable = hiveDictTable;
        this.hiveSrcTable = hiveSrcTable;
        this.output = output;
        this.rowCountPath = rowCountPath;
    }

    public HiveMeta() {
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public HiveDictTable getHiveDictTable() {
        return hiveDictTable;
    }

    public void setHiveDictTable(HiveDictTable hiveDictTable) {
        this.hiveDictTable = hiveDictTable;
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

    @Override
    public String toString() {
        return "HiveMeta{" +
                "hiveDictTable=" + hiveDictTable +
                ", hiveSrcTable=" + hiveSrcTable +
                ", output='" + output + '\'' +
                ", rowCountPath='" + rowCountPath + '\'' +
                '}';
    }
}
