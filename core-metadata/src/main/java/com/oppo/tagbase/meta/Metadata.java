package com.oppo.tagbase.meta;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.RangeSet;
import com.oppo.tagbase.meta.connector.MetadataConnector;
import com.oppo.tagbase.meta.obj.Column;
import com.oppo.tagbase.meta.obj.DB;
import com.oppo.tagbase.meta.obj.Slice;
import com.oppo.tagbase.meta.obj.SliceStatus;
import com.oppo.tagbase.meta.obj.Table;
import com.oppo.tagbase.meta.obj.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Created by wujianchao on 2020/2/5.
 */
public class Metadata {

    private Logger log = LoggerFactory.getLogger(getClass());

    @Inject
    private MetadataConnector metadataConnector;

    /*-------------Metadata initialization part--------------*/

    public void initSchema() throws MetadataException {
        metadataConnector.initSchema();
    }

    /*-------------Metadata DDL part--------------*/

    public void addDb(String dbName, String desc) throws MetadataException {
        metadataConnector.addDb(dbName, desc);
    }

    public void addTable(String dbName,
                            String tableName,
                            String srcDb,
                            String srcTable,
                            String desc,
                            TableType type,
                            List<Column> columnList) throws MetadataException {
        metadataConnector.addTable(dbName,
                tableName,
                srcDb,
                srcTable,
                desc,
                type,
                columnList);
    }

    /*-------------Metadata API for data building--------------*/

    public Table getTable(String dbName, String tableName) throws MetadataException {
        return metadataConnector.getTable(dbName, tableName);
    }

    public void addSlice(Slice slice) {
        metadataConnector.addSlice(slice);
    }

    public Slice getSlice(String sink){
        return metadataConnector.getSlices(sink);
    }

    public void updateSliceStatus(long id, long tableId, SliceStatus status) throws MetadataException {
        metadataConnector.updateSliceStatus(id, tableId, status);
    }

    public void updateSliceSinkStatistics(long id, long sinkSizeMb, long sinkCount) throws MetadataException {
        metadataConnector.updateSliceSinkStatistics(id, sinkSizeMb, sinkCount);
    }

    /*-------------Metadata API for query--------------*/

    /**
     * get all slices of a table
     */
    public List<Slice> getSlices(String dbName, String tableName) throws MetadataException {
        return metadataConnector.getSlices(dbName, tableName);
    }

    /**
     * get slices with filter
     */
    //TODO replace RangeSet and Range with self defined implementations for it is too important.
    public List<Slice> getSlices(String dbName, String tableName, RangeSet<LocalDateTime> range) throws MetadataException {
        return metadataConnector.getSlices(dbName, tableName, range);
    }

    /**
     * get slices which greater than the value
     */
    @Deprecated
    public List<Slice> getSlicesGT(String dbName, String tableName, LocalDateTime value) throws MetadataException {
        return metadataConnector.getSlicesGT(dbName, tableName, value);
    }

    /**
     * get slices which greater or equal than the value
     */
    @Deprecated
    public List<Slice> getSlicesGE(String dbName, String tableName, LocalDateTime value) throws MetadataException {
        return metadataConnector.getSlicesGE(dbName, tableName, value);
    }

    /**
     * get slices which less than the value
     */
    @Deprecated
    public List<Slice> getSlicesLT(String dbName, String tableName, LocalDateTime value) throws MetadataException {
        return metadataConnector.getSlicesLT(dbName, tableName, value);
    }

    /**
     * get slices which less or equal than the value
     */
    @Deprecated
    public List<Slice> getSlicesLE(String dbName, String tableName, LocalDateTime time) throws MetadataException {
        return metadataConnector.getSlicesLE(dbName, tableName, time);
    }


    /**
     * get slices which between the lower and upper
     */
    @Deprecated
    public List<Slice> getSlicesBetween(String dbName, String tableName, LocalDateTime lower, LocalDateTime upper) throws MetadataException {
        return metadataConnector.getSlicesBetween(dbName, tableName, lower, upper);
    }


    /*-------------Metadata API for checking status--------------*/

    public DB getDb(String dbName) throws MetadataException {
        return metadataConnector.getDb(dbName);
    }

    //TODO
    public ImmutableList<DB> listDBs() throws MetadataException {
        return null;
    }

    //TODO
    public ImmutableList<Table> listTables(String dbName) throws MetadataException {
        return null;
    }


}
