package com.oppo.tagbase.meta.connector;

import com.google.common.collect.ImmutableList;
import com.oppo.tagbase.meta.obj.*;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Batch;
import org.jdbi.v3.guava.GuavaPlugin;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.inject.Inject;
import java.util.List;

/**
 * Created by wujianchao on 2020/2/4.
 */
public abstract class MetadataConnector {

    @Inject
    private MetaStoreConnectorConfig config;

    private Jdbi jdbi;

    public MetadataConnector() throws ClassNotFoundException {
        registerJDBCDriver();
        initJdbi();
    }

    private void initJdbi() {
        Jdbi jdbi = Jdbi.create(
                config.getConnectURI(),
                config.getUser(),
                config.getPassword()
        );
        jdbi.installPlugin(new SqlObjectPlugin());
        jdbi.installPlugin(new GuavaPlugin());
    }

    protected abstract void registerJDBCDriver() throws ClassNotFoundException;

    protected Jdbi get() {
        return jdbi;
    }

//    protected <R, X extends Exception> R execute(HandleCallback<R, X> handle, String sql) throws X {
//        return get().withHandle(handle);
//    }
//
//    protected <R, X extends Exception> void executeBatch(HandleCallback<R, X> handle) throws X {
//        Batch batch = handle.createBatch();
//    }

    /*-------------Metadata initialization part--------------*/

    public void initSchema() {
        get().withHandle(handle -> {

            ImmutableList<String> sqls = ImmutableList.of(
                    // create table DB
                    "Create table if not exist DB (\n" +
                            "\tid INTEGER PRIMARY KEY, \n" +
                            "\tname VARCHAR(128)\n" +
                            ")",

                    // create table TBL
                    "Create table if not exist TBL (\n" +
                            "\tid INTEGER PRIMARY KEY, \n" +
                            "\tname VARCHAR(128),\n" +
                            "\tdbId INTEGER,\n" +
                            "\tsrcDb VARCHAR(128),\n" +
                            "\tsrcTable VARCHAR(128),\n" +
                            "\tdesc VARCHAR(128),\n" +
                            "\tlatestSlice  VARCHAR(128),\n" +
                            "\ttype VARCHAR(128)\n" +
                            ")",

                    // create table SLICE
                    "Create table  if not exist SLICE (\n" +
                            "\tid INTEGER PRIMARY KEY, \n" +
                            "\tname VARCHAR(128),\n" +
                            "\ttableId INTEGER,\n" +
                            "\tstatus VARCHAR(128),\n" +
                            "\tsrcTable VARCHAR(128),\n" +
                            "\tsink VARCHAR(128),\n" +
                            "\tshardNum  TINYINT\n" +
                            ")",

                    // create table COLUMN
                    "Create table  if not exist COLUMN (\n" +
                            "\tid INTEGER PRIMARY KEY, \n" +
                            "\tname VARCHAR(128),\n" +
                            "\ttableId INTEGER,\n" +
                            "\tsrcName VARCHAR(128),\n" +
                            "\tindex TINYINT,\n" +
                            "\tdataType VARCHAR(128),\n" +
                            "\ttype TINYINT,\n" +
                            "\tdesc VARCHAR(256)\n" +
                            ")"
            );

            Batch batch = handle.createBatch();
            for (String s : sqls) {
                batch.add(s);
            }
            return batch.execute();
        });
    }



    /*-------------Metadata DDL part--------------*/

    public void createDb(String dbName, String desc) {
        get().withHandle(handle -> {
            String sql = "INSERT INTO DB(name, desc) VALUES (?, ?)";
            return handle.execute(sql, dbName, desc);
        });
    }

    public void createTable(String dbName,
                            String tableName,
                            String srcDb,
                            String srcTable,
                            String desc,
                            TableType type,
                            List<Column> columnList) {
        get().withHandle(handle -> {

            // 1. get dbId
            int dbId = handle.createQuery("Select id from DB where name= :name")
                    .bind("name", dbName)
                    .mapTo(Integer.class)
                    .one();


            // 2. create table
            handle.createUpdate("INSERT INTO " +
                    "TBL(name, dbId, srcDb, srcTable, desc, type) " +
                    "VALUES (:tableName, :dbId, :srcDb, :srcTable, :desc, :type)")
                    .bind("tableName", tableName)
                    .bind("srcDb", srcDb)
                    .bind("srcTable", srcTable)
                    .bind("desc", desc)
                    .bind("type", type)
                    .execute();

            // 3. get tableId
            int tableId = handle.createQuery("Select id from TBL where name=:name and dbId=:dbId")
                    .bind("name", tableName)
                    .bind("dbId", dbId)
                    .mapTo(Integer.class)
                    .one();

            // 4. add columns
            for (Column column : columnList) {
                handle.createUpdate("INSERT INTO " +
                        "COLUMN(tableId, name, srcName, index, dataType, type, desc) " +
                        "values(:tableId, :columnName, :srcName, :index, :dataType, :type, :desc)")
                        .bind("tableId", tableId)
                        .bind("columnName", column.getName())
                        .bind("srcName", column.getSrcName())
                        .bind("index", column.getIndex())
                        .bind("dataType", column.getDataType())
                        .bind("type", column.getType())
                        .bind("desc", column.getDesc())
                        .execute();
            }

            return null;
        });
    }



    /*-------------Metadata API for data building--------------*/

    public Table getTable(String dbName, String tableName) {
        return get().withHandle(handle -> {

            Table table = handle.createQuery("Select TBL.* " +
                    "from TBL join DB on DB.id=TBL.dbId where DB.name=:dbName And TBL.name=tableName")
                    .bind("dbName", dbName)
                    .bind("tableName", tableName)
                    .mapToBean(Table.class)
                    .one();
//                    .orElseThrow(() -> new MetadataException("no TBL named " + tableName));

            return table;
        });
    }


    public void addSlice(Slice slice) {
        get().withHandle(handle -> {

            TableType tableType = handle.createQuery("select type from TBL where id=:tableId")
                    .bind("tableId", slice.getTableId())
                    .mapTo(TableType.class)
                    .one();

            String sqlDisableSlice = String.format("Update SLICE set status=%s where tableId=%d"
                    , SliceStatus.DISABLED
                    , slice.getTableId());

            String sqlAddSlice = String.format("Insert into SLICE(name, tableId, status, sink, shardNum) " +
                            "values(%s, %d, %s, %s, %d)",
                    slice.getName(),
                    slice.getTableId(),
                    slice.getStatus(),
                    slice.getSink(),
                    slice.getShardNum());

            if (tableType == TableType.TAG) {
                return handle.inTransaction(transaction -> {
                    Batch batch = transaction.createBatch();
                    batch.add(sqlDisableSlice);
                    batch.add(sqlAddSlice);
                    return batch.execute();
                });
            } else {
                return handle.execute(sqlAddSlice);
            }
        });
    }

    /*-------------Metadata API for query--------------*/

    public List<Slice> getSlices(String dbName, String tableName) {

        String sqlGetSlices = String.format("Select SLICE.* from " +
                        "DB join TBL on DB.id=TBL.dbId join SLICE on TBL.id=SLICE.tableId " +
                        "where DB.name=%s And TBL .name=%s",
                dbName,
                tableName);

        return get().withHandle(handle -> {
            List<Slice> sliceList = handle.createQuery(sqlGetSlices)
                    .mapToBean(Slice.class)
                    .list();
            return sliceList;
        });
    }

    /*-------------Metadata API for checking status--------------*/

    public DB getDb(String dbName) {
        String sqlGetDb = String.format("Select * from DB where DB.name=%s", dbName);
        return get().withHandle(handle -> {
            DB db = handle.createQuery(sqlGetDb)
                    .mapToBean(DB.class)
                    .one();
            return db;
        });
    }

}
