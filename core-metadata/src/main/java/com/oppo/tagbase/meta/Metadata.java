package com.oppo.tagbase.meta;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.RangeSet;
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

    public void addDb(String dbName, String desc)  {
        metadataConnector.addDb(dbName, desc);
    }

    public void addTable(String dbName,
                            String tableName,
                            String srcDb,
                            String srcTable,
                            String desc,
                            TableType type,
                            List<Column> columnList) {
        metadataConnector.addTable(dbName,
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

    public void updateSliceStatus(long id, long tableId, SliceStatus status) {
        metadataConnector.updateSliceStatus(id, tableId, status);
    }

    public void updateSliceSinkStatistics(long id, long sinkSizeMb, long sinkCount) {
        metadataConnector.updateSliceSinkStatistics(id, sinkSizeMb, sinkCount);
    }

    /*-------------Metadata API for query--------------*/

    /**
     * get all slices of a table
     */
    public List<Slice> getSlices(String dbName, String tableName) {
        return metadataConnector.getSlices(dbName, tableName);
    }

    /**
     * get slices with filter
     */
    //TODO replace RangeSet and Range with self defined implementations for it is too important.
    public List<Slice> getSlices(String dbName, String tableName, RangeSet<Date> range) {
        return metadataConnector.getSlices(dbName, tableName, range);
    }

    /**
     * get slices which greater than the value
     */
    @Deprecated
    public List<Slice> getSlicesGT(String dbName, String tableName, Date value) {
        return metadataConnector.getSlicesGT(dbName, tableName, value);
    }

    /**
     * get slices which greater or equal than the value
     */
    @Deprecated
    public List<Slice> getSlicesGE(String dbName, String tableName, Date value) {
        return metadataConnector.getSlicesGE(dbName, tableName, value);
    }

    /**
     * get slices which less than the value
     */
    @Deprecated
    public List<Slice> getSlicesLT(String dbName, String tableName, Date value) {
        return metadataConnector.getSlicesLT(dbName, tableName, value);
    }

    /**
     * get slices which less or equal than the value
     */
    @Deprecated
    public List<Slice> getSlicesLE(String dbName, String tableName, Date time) {
        return metadataConnector.getSlicesLE(dbName, tableName, time);
    }


    /**
     * get slices which between the lower and upper
     */
    @Deprecated
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


}
