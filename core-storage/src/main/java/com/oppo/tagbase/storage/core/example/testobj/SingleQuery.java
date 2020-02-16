package com.oppo.tagbase.storage.core.example.testobj;


import java.util.List;

/**
 * Created by 71518 on 2020/2/7.
 */
public class SingleQuery{


    private String tableName;

    private String dbName;

    private List<String> dimensions;

    private List<Filter> filters;


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public List<String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(List<String> dimensions) {
        this.dimensions = dimensions;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public void setFilters(List<Filter> filters) {
        this.filters = filters;
    }
}