package com.oppo.tagbase.job.obj;

/**
 * Created by liangjingya on 2020/2/20.
 */
public class HiveMeta {

    private HiveDictTable hiveDictTable;//反向字典表

    private HiveSrcTable hiveSrcTable;//维度表

    private String output;//输出位置，反向字典任务为分区号，bigmap任务为hdfspath

    public HiveMeta(HiveDictTable hiveDictTable, HiveSrcTable hiveSrcTable, String output) {
        this.hiveDictTable = hiveDictTable;
        this.hiveSrcTable = hiveSrcTable;
        this.output = output;
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

    @Override
    public String toString() {
        return "HiveMeta{" +
                "hiveDictTable=" + hiveDictTable +
                ", hiveSrcTable=" + hiveSrcTable +
                ", output='" + output + '\'' +
                '}';
    }
}
