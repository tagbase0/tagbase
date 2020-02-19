package com.oppo.tagbase.storage.core.obj;

import java.util.List;

/**
 * Created by liangjingya on 2020/2/19.
 */
public class SingleQueryManager {

    private String dbName;

    private String tableName;

    private List<String> dimensions;

    private List<Query> dimQueryList;

    private SliceQuery sliceQuery;

    public SingleQueryManager(String dbName, String tableName, List<String> dimensions, List<Query> dimQueryList, SliceQuery sliceQuery) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.dimensions = dimensions;
        this.dimQueryList = dimQueryList;
        this.sliceQuery = sliceQuery;
    }

    public boolean hasdimQuery(){
        return dimQueryList != null;
    }

    public boolean hasSliceQuery(){
        return sliceQuery != null;
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

    public List<Query> getDimQueryList() {
        return dimQueryList;
    }

    public void setDimeQueryList(List<Query> dimQueryList) {
        this.dimQueryList = dimQueryList;
    }

    public SliceQuery getSliceQuery() {
        return sliceQuery;
    }

    public void setSliceQuery(SliceQuery sliceQuery) {
        this.sliceQuery = sliceQuery;
    }
}