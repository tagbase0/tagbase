package com.oppo.tagbase.storage.core.obj;

import java.sql.Date;
import java.util.List;

/**
 * Created by liangjingya on 2020/2/19.
 */
public class QueryHandler {

    private String dbName;

    private String tableName;

    private List<String> dimensions;

    private List<ColumnDomain<String>> dimColumnList;

    private ColumnDomain<Date> sliceColumn;

    public boolean hasDimColumnList(){
        return dimColumnList != null;
    }

    public boolean hasSliceColumn(){
        return sliceColumn != null;
    }

    public QueryHandler(String dbName, String tableName, List<String> dimensions, List<ColumnDomain<String>> dimColumnList, ColumnDomain<Date> sliceColumn) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.dimensions = dimensions;
        this.dimColumnList = dimColumnList;
        this.sliceColumn = sliceColumn;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(List<String> dimensions) {
        this.dimensions = dimensions;
    }

    public List<ColumnDomain<String>> getDimColumnList() {
        return dimColumnList;
    }

    public void setDimColumnList(List<ColumnDomain<String>> dimColumnList) {
        this.dimColumnList = dimColumnList;
    }

    public ColumnDomain<Date> getSliceColumn() {
        return sliceColumn;
    }

    public void setSliceColumn(ColumnDomain<Date> sliceColumn) {
        this.sliceColumn = sliceColumn;
    }
}