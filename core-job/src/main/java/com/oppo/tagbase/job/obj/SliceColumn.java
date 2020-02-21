package com.oppo.tagbase.job.obj;

/**
 * Created by liangjingya on 2020/2/20.
 */
public class SliceColumn {

    private String columnName;

    private String columnValue;

    public SliceColumn(String columnName, String columnValue) {
        this.columnName = columnName;
        this.columnValue = columnValue;
    }

    public SliceColumn() {

    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnValue() {
        return columnValue;
    }

    public void setColumnValue(String columnValue) {
        this.columnValue = columnValue;
    }

    @Override
    public String toString() {
        return "SliceColumn{" +
                "columnName='" + columnName + '\'' +
                ", columnValue='" + columnValue + '\'' +
                '}';
    }
}
