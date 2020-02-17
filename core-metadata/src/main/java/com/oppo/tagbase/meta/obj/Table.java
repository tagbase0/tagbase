package com.oppo.tagbase.meta.obj;

import com.oppo.tagbase.meta.connector.MCE;

import java.util.List;
import java.util.Objects;

/**
 * Created by wujianchao on 2020/2/5.
 */
public class Table {

    private long id;
    private String name;
    private long dbId;
    private String srcDb;
    private String srcTable;
    private String latestSlice;
    private TableType type;
    private String desc;

    private List<Column> columns;


    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getDbId() {
        return dbId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }

    public String getSrcDb() {
        return srcDb;
    }

    public void setSrcDb(String srcDb) {
        this.srcDb = srcDb;
    }

    public String getSrcTable() {
        return srcTable;
    }

    public void setSrcTable(String srcTable) {
        this.srcTable = srcTable;
    }

    public String getLatestSlice() {
        return latestSlice;
    }

    public void setLatestSlice(String latestSlice) {
        this.latestSlice = latestSlice;
    }

    public TableType getType() {
        return type;
    }

    public void setType(TableType type) {
        this.type = type;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public Column getColumn(String columnName) {
        if(columns == null) {
            throw new MCE("The instance initialized without column info.");
        }
        return getColumns().stream()
                .filter(column -> Objects.equals(column.getName(), columnName))
                .findFirst().get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Table table = (Table) o;
        return dbId == table.dbId &&
                Objects.equals(name, table.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dbId);
    }
}
