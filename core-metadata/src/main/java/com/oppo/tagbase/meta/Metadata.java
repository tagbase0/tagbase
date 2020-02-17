package com.oppo.tagbase.meta;

import com.google.common.collect.ImmutableList;
import com.oppo.tagbase.meta.connector.MetadataConnector;
import com.oppo.tagbase.meta.obj.*;

import javax.inject.Inject;
import java.sql.Date;
import java.util.List;

/**
 * Created by wujianchao on 2020/2/5.
 */
public class Metadata {

    @Inject
    private MetadataConnector metadataConnector;

    /*-------------Metadata initialization part--------------*/

    public void initSchema() {
        metadataConnector.initSchema();
    }

    /*-------------Metadata DDL part--------------*/

    public void createDb(String dbName, String desc)  {
        metadataConnector.createDb(dbName, desc);
    }

    public void createTable(String dbName,
                            String tableName,
                            String srcDb,
                            String srcTable,
                            String desc,
                            TableType type,
                            List<Column> columnList) {
        metadataConnector.createTable(dbName,
                tableName,
                srcDb,
                srcTable,
                desc,
                type,
                columnList);
    }

    /*-------------Metadata API for data building--------------*/

    public Table getTable(String dbName, String tableName) {
        return metadataConnector.getTable(dbName, tableName);
    }

    public void addSlice(Slice slice) {
        metadataConnector.addSlice(slice);
    }

    /*-------------Metadata API for query--------------*/

    /**
     * get all slices of a table
     */
    public List<Slice> getSlices(String dbName, String tableName) {
        return metadataConnector.getSlices(dbName, tableName);
    }

    /**
     * get slices which greater than the value
     */
    public List<Slice> getSlicesGT(String dbName, String tableName, Date value) {
        return metadataConnector.getSlicesGT(dbName, tableName, value);
    }

    /**
     * get slices which greater or equal than the value
     */
    public List<Slice> getSlicesGE(String dbName, String tableName, Date value) {
        return metadataConnector.getSlicesGE(dbName, tableName, value);
    }

    /**
     * get slices which less than the value
     */
    public List<Slice> getSlicesLT(String dbName, String tableName, Date value) {
        return metadataConnector.getSlicesLT(dbName, tableName, value);
    }

    /**
     * get slices which less or equal than the value
     */
    public List<Slice> getSlicesLE(String dbName, String tableName, Date time) {
        return metadataConnector.getSlicesLE(dbName, tableName, time);
    }


    /**
     * get slices which between the lower and upper
     */
    public List<Slice> getSlicesBetween(String dbName, String tableName, Date lower, Date upper) {
        return metadataConnector.getSlicesBetween(dbName, tableName, lower, upper);
    }


    /*-------------Metadata API for checking status--------------*/

    public DB getDb(String dbName) {
        return metadataConnector.getDb(dbName);
    }

    //TODO
    public ImmutableList<DB> listDBs() {
        return null;
    }

    //TODO
    public ImmutableList<Table> listTables(String dbName) {
        return null;
    }

    //TODO
    public void createDB(String dbName, String desc) {

    }


}
