package com.oppo.tagbase.meta.obj;

import com.oppo.tagbase.meta.type.DataType;

import java.util.Objects;

/**
 * Created by wujianchao on 2020/2/6.
 */
public class Column {

    private long id;
    private long tableId;
    private String name;
    private String srcName;
    private int index;
    private DataType dataType = DataType.STRING;
    private ColumnType type;
    private String desc;
    private ResourceColType srcDataType;
    /**
     * If column is Hive partition column and hive data type is string,
     * we must know the date format.
     */
    private ColDateFormat srcPartColDateFormat;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSrcName() {
        return srcName;
    }

    public void setSrcName(String srcName) {
        this.srcName = srcName;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public ColumnType getType() {
        return type;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public ResourceColType getSrcDataType() {
        return srcDataType;
    }

    public void setSrcDataType(ResourceColType srcDataType) {
        this.srcDataType = srcDataType;
    }

    public ColDateFormat getSrcPartColDateFormat() {
        return srcPartColDateFormat;
    }

    public void setSrcPartColDateFormat(ColDateFormat srcPartColDateFormat) {
        this.srcPartColDateFormat = srcPartColDateFormat;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Column column = (Column) o;
        return tableId == column.tableId &&
                Objects.equals(name, column.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, name);
    }

    @Override
    public String toString() {
        return "Column{" +
                "id=" + id +
                ", tableId=" + tableId +
                ", name='" + name + '\'' +
                ", srcName='" + srcName + '\'' +
                ", index=" + index +
                ", dataType=" + dataType +
                ", type=" + type +
                ", desc='" + desc + '\'' +
                ", srcDataType='" + srcDataType + '\'' +
                ", srcPartColDateFormat='" + srcPartColDateFormat + '\'' +
                '}';
    }
}
