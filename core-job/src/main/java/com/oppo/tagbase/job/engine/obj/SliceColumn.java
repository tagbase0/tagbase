package com.oppo.tagbase.job.engine.obj;

/**
 * Created by liangjingya on 2020/2/20.
 */
public class SliceColumn {

    private String columnName;

    private String columnValueLeft;

    private String columnValueRight;


    public SliceColumn() {

    }

    public SliceColumn(String columnName, String columnValueLeft, String columnValueRight) {
        this.columnName = columnName;
        this.columnValueLeft = columnValueLeft;
        this.columnValueRight = columnValueRight;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnValueLeft() {
        return columnValueLeft;
    }

    public void setColumnValueLeft(String columnValueLeft) {
        this.columnValueLeft = columnValueLeft;
    }

    public String getColumnValueRight() {
        return columnValueRight;
    }

    public void setColumnValueRight(String columnValueRight) {
        this.columnValueRight = columnValueRight;
    }

    @Override
    public String toString() {
        return "SliceColumn{" +
                "columnName='" + columnName + '\'' +
                ", columnValueLeft='" + columnValueLeft + '\'' +
                ", columnValueRight='" + columnValueRight + '\'' +
                '}';
    }
}
