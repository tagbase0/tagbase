package com.oppo.tagbase.meta.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.oppo.tagbase.meta.obj.*;
import com.oppo.tagbase.meta.util.SqlDateUtil;
import org.jdbi.v3.core.HandleCallback;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Batch;
import org.jdbi.v3.guava.GuavaPlugin;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.inject.Inject;
import java.sql.Date;
import java.util.List;
import java.util.stream.Collectors;

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
        jdbi = Jdbi.create(
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

    protected <R> R submit(HandleCallback<R, MCE> handle) {
        return get().withHandle(handle);
    }


    /*-------------Metadata initialization part--------------*/

    public void initSchema() {
        submit(handle -> {

            ImmutableList<String> sqlList = ImmutableList.of(
                    // create table DB
                    "Create table if not exist DB (\n" +
                            "\tid INTEGER PRIMARY KEY AUTO_INCREMENT, \n" +
                            "\tname VARCHAR(128) NOT NULL UNIQUE\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1",

                    // create table TBL
                    "Create table if not exist TBL (\n" +
                            "\tid INTEGER PRIMARY KEY AUTO_INCREMENT, \n" +
                            "\tname VARCHAR(128),\n" +
                            "\tdbId INTEGER,\n" +
                            "\tsrcDb VARCHAR(128),\n" +
                            "\tsrcTable VARCHAR(128),\n" +
                            "\tdesc VARCHAR(128),\n" +
                            "\tlatestSlice  VARCHAR(128),\n" +
                            "\ttype VARCHAR(128)\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1",

                    // add index
                    "CREATE UNIQUE INDEX nameAndDbId ON TBL(name, dbId)",

                    // create table COLUMN
                    "Create table  if not exist COLUMN (\n" +
                            "\tid INTEGER PRIMARY KEY AUTO_INCREMENT, \n" +
                            "\tname VARCHAR(128),\n" +
                            "\ttableId INTEGER,\n" +
                            "\tsrcName VARCHAR(128),\n" +
                            "\tindex TINYINT,\n" +
                            "\tdataType VARCHAR(128),\n" +
                            "\ttype TINYINT,\n" +
                            "\tdesc VARCHAR(256)\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1",

                    // add index
                    "CREATE UNIQUE INDEX nameAndTableId ON COLUMN(name, tableId)",

                    // create table SLICE
                    "Create table  if not exist SLICE (\n" +
                            "\tid INTEGER PRIMARY KEY AUTO_INCREMENT, \n" +
                            "\tstartTime DATETIME,\n" +
                            "\tendTime DATETIME,\n" +
                            "\ttableId INTEGER,\n" +
                            "\tstatus VARCHAR(128),\n" +
                            "\tsrcTable VARCHAR(128),\n" +
                            "\tsink VARCHAR(128) unique,\n" +
                            "\tshardNum  TINYINT,\n" +
                            "\tsrcSizeMb  BIGINT,\n" +
                            "\tsrcCount  BIGINT,\n" +
                            "\tsinkSizeMb  BIGINT,\n" +
                            "\tsinkCount  BIGINT\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1",

                    // add index
                    "CREATE UNIQUE INDEX nameAndTableId ON SLICE(tableId, startTime)",
                    "CREATE UNIQUE INDEX nameAndTableId ON SLICE(tableId, endTime)",

                    // create table JOB
                    "Create table  if not exist JOB (\n" +
                            "\tid VARCHAR(128) PRIMARY KEY, \n" +
                            "\tname VARCHAR(256),\n" +
                            "\tdbName VARCHAR(128),\n" +
                            "\ttableName VARCHAR(128),\n" +
                            "\tstartTime DATETIME,\n" +
                            "\tdataLowerTime VARCHAR(128),\n" +
                            "\tdataUpperTime DATETIME,\n" +
                            "\tendTime DATETIME,\n" +
                            "\tlatestTask VARCHAR(128),\n" +
                            "\tstate VARCHAR(128),\n" +
                            "\ttype  VARCHAR(128)\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1",

                    // create table TASK
                    "Create table  if not exist TASK (\n" +
                            "\tid VARCHAR(128) PRIMARY KEY, \n" +
                            "\tname VARCHAR(256),\n" +
                            "\tjobId VARCHAR(128),\n" +
                            "\tappId VARCHAR(128),\n" +
                            "\tstartTime DATETIME,\n" +
                            "\tendTime DATETIME,\n" +
                            "\tstep tinyint,\n" +
                            "\tstate VARCHAR(128),\n" +
                            "\toutput VARCHAR(1024)\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1",

                    // create table DICT
                    "Create table  if not exist DICT (\n" +
                            "\tid INTEGER PRIMARY KEY AUTO_INCREMENT, \n" +
                            "\tversion VARCHAR(128),\n" +
                            "\tstatus VARCHAR(128),\n" +
                            "\tlocation VARCHAR(512),\n" +
                            "\telementCount BIGINT,\n" +
                            "\tendTime createDate,\n" +
                            "\ttype VARCHAR(128)\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1"
            );

            Batch batch = handle.createBatch();
            for (String s : sqlList) {
                batch.add(s);
            }
            return batch.execute();
        });
    }



    /*-------------Metadata DDL part--------------*/

    public void addDb(String dbName, String desc) {
        submit(handle -> {
            String sql = "INSERT INTO DB(name, desc) VALUES (?, ?)";
            return handle.execute(sql, dbName, desc);
        });
    }

    public void addTable(String dbName,
                         String tableName,
                         String srcDb,
                         String srcTable,
                         String desc,
                         TableType type,
                         List<Column> columnList) {
        submit(handle -> {

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
        return submit(handle -> {

            Table table = handle.createQuery("Select TBL.* " +
                    "from TBL join DB on DB.id=TBL.dbId where DB.name=:dbName And TBL.name=tableName")
                    .bind("dbName", dbName)
                    .bind("tableName", tableName)
                    .mapToBean(Table.class)
                    .one();
//                    .orElseThrow(() -> new MetadataException("no TBL named " + tableName));

            List<Column> columnList = handle.createQuery("select * from COLUMN where tableId=:tableId")
                    .bind("tableId", table.getId())
                    .mapToBean(Column.class)
                    .list();

            table.setColumns(columnList);
            return table;
        });
    }


    //TODO transaction
    public void addSlice(Slice slice) {
        submit(handle -> {

            if(slice.getStatus() != SliceStatus.BUILDING) {
                throw new MCE("Newly added slice must be BUILDING status.");
            }

            TableType tableType = handle.createQuery("select type from TBL where id=:tableId")
                    .bind("tableId", slice.getTableId())
                    .mapTo(TableType.class)
                    .one();

//            String sqlDisableSlice = String.format("Update SLICE set status='%s' where tableId=%d and status=%s"
//                    , SliceStatus.DISABLED
//                    , slice.getTableId());
//
//            String sqlAddSlice = String.format("Insert into SLICE(name, tableId, status, sink, shardNum) " +
//                            "values('%s', %d, '%s', '%s', %d)",
//                    slice.getStartTime(),
//                    slice.getTableId(),
//                    slice.getStatus(),
//                    slice.getSink(),
//                    slice.getShardNum());
//
//            if (tableType == TableType.TAG) {
//                return handle.inTransaction(transaction -> {
//                    Batch batch = transaction.createBatch();
//                    batch.add(sqlDisableSlice);
//                    batch.add(sqlAddSlice);
//                    return batch.execute();
//                });
//            } else {
//                return handle.execute(sqlAddSlice);
//            }

            if(tableType == TableType.TAG) {
                handle.execute("Update SLICE set status=? where tableId=? and status=?",
                        SliceStatus.DISABLED,
                        slice.getTableId(),
                        SliceStatus.READY);
            }

            return handle.execute("Insert into SLICE(startTime, endTime, tableId, status, sink, shardNum, srcSizeMb, srcCount, sinkSizeMb, sinkCount) " +
                            "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    slice.getStartTime(),
                    slice.getEndTime(),
                    slice.getTableId(),
                    slice.getStatus(),
                    slice.getSink(),
                    slice.getShardNum(),
                    slice.getSrcSizeMb(),
                    slice.getSrcCount(),
                    slice.getSinkSizeMb(),
                    slice.getSinkCount()
            );
        });
    }

    public void updateSliceStatus(long id, long tableId, SliceStatus status) {

        submit(handle -> {

            if (status == SliceStatus.READY) {
                TableType tableType = handle.createQuery("select type from TBL where id=:tableId")
                        .bind("tableId", tableId)
                        .mapTo(TableType.class)
                        .one();
                if(tableType == TableType.TAG) {
                    handle.execute("Update SLICE set status=? where tableId=? and status=?",
                            SliceStatus.DISABLED,
                            tableId,
                            status);
                }
            }

            return handle.execute("Update SLICE set status=? where id=?",
                    status,
                    tableId);
        });
    }

    public void updateSliceSinkStatistics(long id, long sinkSizeMb, long sinkCount) {
        submit(handle -> handle.execute("Update SLICE set sinkSizeMb=? and sinkCount=? where id=?",
                    sinkSizeMb,
                    sinkCount,
                    id)
        );
    }



    /*-------------Metadata API for query--------------*/

    public List<Slice> getSlices(String dbName, String tableName) {

        return submit(handle -> handle.createQuery("Select SLICE.* from " +
                "DB join TBL on DB.id=TBL.dbId join SLICE on TBL.id=SLICE.tableId " +
                "where DB.name=:dbName And TBL.name=:tableName and SLICE.status=:sliceStatus")
                .bind("dbName", dbName)
                .bind("tableName", tableName)
                .bind("sliceStatus", SliceStatus.READY)
                .mapToBean(Slice.class)
                .list());
    }

    //TODO use a more efficient fashion
    public List<Slice> getSlices(String dbName, String tableName, RangeSet<Date> range) {
        List<Slice> sliceList = getSlices(dbName, tableName);
        List<Slice> ret = sliceList.stream()
                .filter(slice -> range.encloses(Range.closedOpen(slice.getStartTime(), slice.getEndTime())))
                .collect(Collectors.toList());
        return ret;
    }

    /**
     * get slices which greater than the value
     */
    @Deprecated
    public List<Slice> getSlicesGT(String dbName, String tableName, Date time) {

        return submit(handle -> handle.createQuery("Select SLICE.* from " +
                "DB join TBL on DB.id=TBL.dbId join SLICE on TBL.id=SLICE.tableId " +
                "where DB.name=:dbName And TBL.name=:tableName and SLICE.status=:sliceStatus and " +
                "(SLICE.startTime>:time or SLICE.endTime<:time)")
                .bind("dbName", dbName)
                .bind("tableName", tableName)
                .bind("sliceStatus", SliceStatus.READY)
                .bind("time", time)
                .mapToBean(Slice.class)
                .list());
    }

    /**
     * get slices which greater than or equal the value
     */
    @Deprecated
    public List<Slice> getSlicesGE(String dbName, String tableName, Date time) {
        return submit(handle -> handle.createQuery("Select SLICE.* from " +
                "DB join TBL on DB.id=TBL.dbId join SLICE on TBL.id=SLICE.tableId " +
                "where DB.name=:dbName And TBL.name=:tableName and SLICE.status=:sliceStatus and " +
                "(SLICE.startTime>=:time or SLICE.endTime>:time)")
                .bind("dbName", dbName)
                .bind("tableName", tableName)
                .bind("sliceStatus", SliceStatus.READY)
                .bind("time", time)
                .mapToBean(Slice.class)
                .list());

    }

    /**
     * get slices which less than the value
     */
    @Deprecated
    public List<Slice> getSlicesLT(String dbName, String tableName, Date time) {

        return submit(handle -> handle.createQuery("Select SLICE.* from " +
                "DB join TBL on DB.id=TBL.dbId join SLICE on TBL.id=SLICE.tableId " +
                "where DB.name=:dbName And TBL.name=:tableName and SLICE.status=:sliceStatus and" +
                " (SLICE.endTime<=:time)")
                .bind("dbName", dbName)
                .bind("tableName", tableName)
                .bind("sliceStatus", SliceStatus.READY)
                .bind("time", time)
                .mapToBean(Slice.class)
                .list());
    }

    /**
     * get slices which less or equal than the value
     */
    @Deprecated
    public List<Slice> getSlicesLE(String dbName, String tableName, Date time) {
        return submit(handle -> handle.createQuery("Select SLICE.* from " +
                "DB join TBL on DB.id=TBL.dbId join SLICE on TBL.id=SLICE.tableId " +
                "where DB.name=:dbName And TBL.name=:tableName and SLICE.status=:sliceStatus and" +
                " SLICE.startTime<=:time")
                .bind("dbName", dbName)
                .bind("tableName", tableName)
                .bind("sliceStatus", SliceStatus.READY)
                .bind("time", time)
                .mapToBean(Slice.class)
                .list());
    }


    /**
     * get slices which between the lower and upper
     */
    @Deprecated
    public List<Slice> getSlicesBetween(String dbName, String tableName, Date lower, Date upper) {
        Date nextUpperDate = SqlDateUtil.addSomeDays(upper, 1);

        return submit(handle -> handle.createQuery("Select SLICE.* from " +
                "DB join TBL on DB.id=TBL.dbId join SLICE on TBL.id=SLICE.tableId " +
                "where DB.name=:dbName And TBL.name=:tableName and SLICE.status=:sliceStatus and" +
                " SLICE.startTime>=:lower and SLICE.endTIme<=:upper")
                .bind("dbName", dbName)
                .bind("tableName", tableName)
                .bind("sliceStatus", SliceStatus.READY)
                .bind("lower", lower)
                .bind("upper", nextUpperDate)
                .mapToBean(Slice.class)
                .list());
    }

    /*-------------Metadata API for checking status--------------*/

    public DB getDb(String dbName) {
        return submit(handle -> handle.createQuery("Select * from DB where DB.name=:dbName")
                .bind("dbName", dbName)
                .mapToBean(DB.class)
                .one());
    }


    /*-------------Metadata API for Job module--------------*/

    public void addJob(Job job) {
        submit(handle -> {
            String sql = "INSERT INTO JOB(id, name, dbName, tableName, startTime, endTime, dataLowerTime, dataUpperTime, latestTask, state, type) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            return handle.execute(sql,
                    job.getId(),
                    job.getName(),
                    job.getDbName(),
                    job.getTableName(),
                    job.getStartTime(),
                    job.getEndTime(),
                    job.getDataLowerTime(),
                    job.getDataUpperTime(),
                    job.getLatestTask(),
                    job.getState(),
                    job.getType());
        });
    }

    public void deleteJOb(String jobId) {
        submit(handle -> {
            return handle.inTransaction(transaction -> {
                String sqlDeleteJob = String.format("DELETE FROM JOB where id='%s'", jobId);
                String sqlDeleteTask = String.format("DELETE FROM TASK where jobId='%s'", jobId);
                Batch batch = transaction.createBatch();
                batch.add(sqlDeleteJob);
                batch.add(sqlDeleteTask);
                return batch.execute();
            });
        });
    }

    public void completeJOb(String jobId, JobState state, Date endTime) {
        submit(handle -> {
            String sql = "Update JOB set state=? and endTime=? where id=?";
            return handle.execute(sql, state, endTime, jobId);
        });
    }

    public Job getJob(String jobId) {
        return submit(handle -> {

            Job job = handle.createQuery("Select JOB.* from JOB where JOB.id=:jobId")
                    .bind("jobId", jobId)
                    .mapToBean(Job.class)
                    .one();

            List<Task> tasks = handle.createQuery("select * from TASK where jobId=:jobId order by step")
                    .bind("jobId", jobId)
                    .mapToBean(Task.class)
                    .list();

            job.setTasks(tasks);
            return job;
        });

    }


    public void addTask(Task task) {
        submit(handle -> {
            String sql = "INSERT INTO TASK(id, name, jobId, appId, startTime, endTime, step, state, output) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            return handle.execute(sql,
                    task.getId(),
                    task.getName(),
                    task.getJobId(),
                    task.getAppId(),
                    task.getStartTime(),
                    task.getEndTime(),
                    task.getStep(),
                    task.getState(),
                    task.getOutput()
            );
        });
    }

    public void completeTask(String taskId, TaskState state, Date endTime, String output) {
        submit(handle -> {
            String sql = "Update TASK set state=? and endTime=? and output=? where id=?";
            return handle.execute(sql, state, endTime, output, taskId);
        });
    }


    /*-------------Metadata API for Dict module--------------*/

    //TODO put two sql in transaction
    public void addDict(Dict dict) {

        submit(handle -> {

            if (DictStatus.UNUSED == dict.getStatus()) {
                throw new MCE("Newly added dict must be READY status.");
            }

            String sqlDisableDict = "UPDATE DICT set status=? where status=?";
            handle.execute(sqlDisableDict, DictStatus.UNUSED, DictStatus.READY);

            String sqlAddDict = "INSERT INTO DICT(version, status, location, elementCount, createDate, type) " +
                    "VALUES (?, ?, ?, ?, ?, ?)";

            return handle.execute(sqlAddDict,
                    dict.getVersion(),
                    dict.getStatus(),
                    dict.getLocation(),
                    dict.getElementCount(),
                    dict.getCreateDate(),
                    dict.getType());
        });
    }

    public Dict getDict() {
        return submit(handle -> handle.createQuery("SELECT * from DICT where status=:status")
                .bind("status", DictStatus.READY)
                .mapToBean(Dict.class)
                .one());
    }

    public long getDictElementCount() {
        return submit(handle -> handle.createQuery("SELECT elementCount from DICT where status=:status")
                .bind("status", DictStatus.READY)
                .mapTo(Long.class)
                .one());
    }


}
