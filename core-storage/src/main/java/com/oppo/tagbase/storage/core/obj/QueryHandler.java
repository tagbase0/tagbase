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

    private List<ColumnDomain<String>> filterColumnList;

    private ColumnDomain<Date> sliceColumn;

    private String queryId;

    public boolean hasFilterColumnList(){
        return filterColumnList != null;
    }

    public boolean hasSliceColumn(){
        return sliceColumn != null;
    }

    public QueryHandler(String dbName, String tableName, List<String> dimensions, List<ColumnDomain<String>> filterColumnList, ColumnDomain<Date> sliceColumn, String queryId) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.dimensions = dimensions;
        this.filterColumnList = filterColumnList;
        this.sliceColumn = sliceColumn;
        this.queryId = queryId;
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

    public List<ColumnDomain<String>> getFilterColumnList() {
        return filterColumnList;
    }

    public void setFilterColumnList(List<ColumnDomain<String>> filterColumnList) {
        this.filterColumnList = filterColumnList;
    }

    public ColumnDomain<Date> getSliceColumn() {
        return sliceColumn;
    }

    public void setSliceColumn(ColumnDomain<Date> sliceColumn) {
        this.sliceColumn = sliceColumn;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }
}