package com.oppo.tagbase.storage.core.example.testobj;


import java.util.List;

/**
 * Created by wujianchao on 2020/2/5.
 */
public class Table {

    private String name;
//    private String latestSlice;
    private TableType type;
    private List<Column> columns;

    public Table(String name, TableType type, List<Column> columns) {
        this.name = name;
        this.type = type;
        this.columns = columns;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TableType getType() {
        return type;
    }

    public void setType(TableType type) {
        this.type = type;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }
}
