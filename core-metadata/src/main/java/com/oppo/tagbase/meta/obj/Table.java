package com.oppo.tagbase.meta.obj;

import com.google.common.base.Preconditions;

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

    //TODO 数据源 ，目前都是hive
    private TableResourceType srcType;

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

    public TableResourceType getSrcType() {
        return srcType;
    }

    public void setSrcType(TableResourceType srcType) {
        this.srcType = srcType;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        Preconditions.checkNotNull(columns, "Table columns can not be null");
        this.columns = columns;
    }

    /**
     * Get column of the table, as to TAG table slice column is a virtual column and is invisible
     *
     * @return column, or null if column not exist.
     */
    public Column getColumn(String columnName) {
        return getColumns().stream()
                .filter(column -> Objects.equals(column.getName(), columnName))
                .filter(column -> !(TableType.TAG == getType() && ColumnType.SLICE_COLUMN == column.getType()))
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
        return dbId == table.getDbId() && Objects.equals(name, table.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbId, name);
    }

    @Override
    public String toString() {
        return "Table{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", dbId=" + dbId +
                ", srcDb='" + srcDb + '\'' +
                ", srcTable='" + srcTable + '\'' +
                ", latestSlice='" + latestSlice + '\'' +
                ", type=" + type +
                ", desc='" + desc + '\'' +
                ", srcType=" + srcType +
                ", columns=" + columns +
                '}';
    }
}
