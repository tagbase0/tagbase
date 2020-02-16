package com.oppo.tagbase.storage.core.example.testobj;


/**
 * Created by wujianchao on 2020/2/6.
 */
public class Column {

    private String name;
    private int index;
    private ColumnType type;

    public Column(String name, int index, ColumnType type) {
        this.name = name;
        this.index = index;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public ColumnType getType() {
        return type;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }
}
