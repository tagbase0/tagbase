package com.oppo.tagbase.meta.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.oppo.tagbase.meta.MetadataErrorCode;
import com.oppo.tagbase.meta.MetadataException;
import com.oppo.tagbase.meta.obj.*;
import com.oppo.tagbase.meta.util.RangeUtil;
import org.jdbi.v3.core.HandleCallback;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Batch;

import javax.inject.Inject;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by wujianchao on 2020/2/4.
 */
public abstract class MetadataConnector {

    @Inject
    private MetaStoreConnectorConfig config;

    @Inject
    private Jdbi jdbi;

    public MetadataConnector() throws ClassNotFoundException {
        registerJDBCDriver();
    }

    protected abstract void registerJDBCDriver() throws ClassNotFoundException;

    protected Jdbi get() {
        return jdbi;
    }

    protected <R> R submit(HandleCallback<R, MetadataException> handle) {
        return get().withHandle(handle);
    }


    /*-------------Metadata initialization part--------------*/

    public void initSchema() {
        submit(handle -> {

            ImmutableList<String> sqlList = ImmutableList.of(
                    // create table DB
                    "Create table if not exists `DB` (\n" +
                            "\t`id` INTEGER PRIMARY KEY AUTO_INCREMENT, \n" +
                            "\t`name` VARCHAR(128) NOT NULL UNIQUE, \n" +
                            "\t`desc` VARCHAR(128) \n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1",

                    // create table TBL
                    "Create table if not exists `TBL` (\n" +
                            "\t`id` INTEGER PRIMARY KEY AUTO_INCREMENT, \n" +
                            "\t`name` VARCHAR(128),\n" +
                            "\t`dbId` INTEGER,\n" +
                            "\t`srcDb` VARCHAR(128),\n" +
                            "\t`srcTable` VARCHAR(128),\n" +
                            "\t`desc` VARCHAR(128),\n" +
                            "\t`latestSlice`  VARCHAR(128),\n" +
                            "\t`type` VARCHAR(128)\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1",

                    // add index
                    "CREATE UNIQUE INDEX nameAndDbId ON `TBL`(`name`, `dbId`)",

                    // create table COLUMN
                    "Create table  if not exists `COLUMN` (\n" +
                            "\t`id` INTEGER PRIMARY KEY AUTO_INCREMENT, \n" +
                            "\t`name` VARCHAR(128),\n" +
                            "\t`tableId` INTEGER,\n" +
                            "\t`srcName` VARCHAR(128),\n" +
                            "\t`index` TINYINT,\n" +
                            "\t`dataType` VARCHAR(128),\n" +
                            "\t`type` VARCHAR(128),\n" +
                            "\t`desc` VARCHAR(256)\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1",

                    // add index
                    "CREATE UNIQUE INDEX nameAndTableId ON `COLUMN`(`name`, `tableId`)",
//

                    // create table SLICE
                    "Create table  if not exists `SLICE` (\n" +
                            "\t`id` INTEGER PRIMARY KEY AUTO_INCREMENT, \n" +
                            "\t`startTime` DATETIME,\n" +
                            "\t`endTime` DATETIME,\n" +
                            "\t`tableId` INTEGER,\n" +
                            "\t`status` VARCHAR(128),\n" +
                            "\t`sink` VARCHAR(128) unique,\n" +
                            "\t`shardNum`  TINYINT,\n" +
                            "\t`srcSizeMb`  BIGINT,\n" +
                            "\t`srcCount`  BIGINT,\n" +
                            "\t`sinkSizeMb`  BIGINT,\n" +
                            "\t`sinkCount`  BIGINT\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1",

                    // add index
                    "CREATE UNIQUE INDEX tableIdAndStartTime ON `SLICE`(`tableId`, `startTime`)",
                    "CREATE UNIQUE INDEX tableIdAndEndTime ON `SLICE`(`tableId`, `endTime`)",

                    // create table JOB
                    "Create table  if not exists `JOB` (\n" +
                            "\t`id` VARCHAR(128) PRIMARY KEY, \n" +
                            "\t`name` VARCHAR(256),\n" +
                            "\t`dbName` VARCHAR(128),\n" +
                            "\t`tableName` VARCHAR(128),\n" +
                            "\t`startTime` DATETIME,\n" +
                            "\t`endTime` DATETIME,\n" +
                            "\t`dataLowerTime` VARCHAR(128),\n" +
                            "\t`dataUpperTime` DATETIME,\n" +
                            "\t`latestTask` VARCHAR(128),\n" +
                            "\t`state` VARCHAR(128),\n" +
                            "\t`type`  VARCHAR(128)\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1",

                    // create table TASK
                    "Create table  if not exists `TASK` (\n" +
                            "\t`id` VARCHAR(128) PRIMARY KEY, \n" +
                            "\t`name` VARCHAR(256),\n" +
                            "\t`jobId` VARCHAR(128),\n" +
                            "\t`appId` VARCHAR(128),\n" +
                            "\t`startTime` DATETIME,\n" +
                            "\t`endTime` DATETIME,\n" +
                            "\t`step` tinyint,\n" +
                            "\t`state` VARCHAR(128),\n" +
                            "\t`output` VARCHAR(1024)\n" +
                            ") ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1",

                    // create table DICT
                    "Create table  if not exists `DICT` (\n" +
                            "\t`id` INTEGER PRIMARY KEY AUTO_INCREMENT, \n" +
                            "\t`version` VARCHAR(128),\n" +
                            "\t`status` VARCHAR(128),\n" +
                            "\t`location` VARCHAR(512),\n" +
                            "\t`elementCount` BIGINT,\n" +
                            "\t`createDate` DATETIME,\n" +
                            "\t`type` VARCHAR(128)\n" +
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
            String sql = "INSERT INTO `DB`(`name`, `desc`) VALUES (?, ?)";
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
            int dbId = handle.createQuery("Select `id` from `DB` where `name`= :name")
                    .bind("name", dbName)
                    .mapTo(Integer.class)
                    .one();


            // 2. create table
            handle.createUpdate("INSERT INTO " +
                    "`TBL`(`name`, `dbId`, `srcDb`, `srcTable`, `desc`, `type`) " +
                    "VALUES (:tableName, :dbId, :srcDb, :srcTable, :desc, :type)")
                    .bind("tableName", tableName)
                    .bind("dbId", dbId)
                    .bind("srcDb", srcDb)
                    .bind("srcTable", srcTable)
                    .bind("desc", desc)
                    .bind("type", type)
                    .execute();

            // 3. get tableId
            int tableId = handle.createQuery("Select id from  `TBL` where `name`=:name and `dbId`=:dbId")
                    .bind("name", tableName)
                    .bind("dbId", dbId)
                    .mapTo(Integer.class)
                    .one();

            // 4. add columns
            for (Column column : columnList) {
                handle.createUpdate("INSERT INTO " +
                        "`COLUMN`(`tableId`, `name`, `srcName`, `index`, `dataType`, `type`, `desc`) " +
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

            Table table = handle.createQuery("Select `TBL`.* " +
                    "from `TBL` join `DB` on `DB`.id=`TBL`.`dbId` where `DB`.`name`=:dbName And `TBL`.`name`=:tableName")
                    .bind("dbName", dbName)
                    .bind("tableName", tableName)
                    .mapToBean(Table.class)
                    .one();
//                    .orElseThrow(() -> new MetadataException("no TBL named " + tableName));

            List<Column> columnList = handle.createQuery("select * from `COLUMN` where `tableId`=:tableId")
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

            if (slice.getStatus() != SliceStatus.READY) {
                throw new MetadataException(MetadataErrorCode.METADATA_ERROR, "Newly added slice must be READY status.");
            }

            TableType tableType = handle.createQuery("select `type` from `TBL` where `id`=:tableId")
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

            if (tableType == TableType.TAG) {
                handle.execute("Update `SLICE` set `status`=? where `tableId`=? and `status`=?",
                        SliceStatus.DISABLED,
                        slice.getTableId(),
                        SliceStatus.READY);
            }

            return handle.execute("Insert into `SLICE`(`startTime`, `endTime`, `tableId`, `status`, `sink`, `shardNum`, `srcSizeMb`, `srcCount`, `sinkSizeMb`, `sinkCount`) " +
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
                TableType tableType = handle.createQuery("select `type` from `TBL` where `id`=:tableId")
                        .bind("tableId", tableId)
                        .mapTo(TableType.class)
                        .one();
                if (tableType == TableType.TAG) {
                    handle.execute("Update `SLICE` set `status`=? where `tableId`=? and `status`=?",
                            SliceStatus.DISABLED,
                            tableId,
                            status);
                }
            }

            return handle.execute("Update `SLICE` set `status`=? where `id`=?",
                    status,
                    id);
        });
    }

    public void updateSliceSinkStatistics(long id, long sinkSizeMb, long sinkCount) {
        submit(handle -> handle.execute("Update `SLICE` set `sinkSizeMb`=? , `sinkCount`=? where `id`=?",
                sinkSizeMb,
                sinkCount,
                id)
        );
    }

    public Slice getSlices(String sink) {

        return submit(handle -> handle.createQuery("Select `SLICE`.* from SLICE where sink =:sink")
                .bind("sink", sink)
                .mapToBean(Slice.class)
                .one());
    }



    /*-------------Metadata API for query--------------*/

    public List<Slice> getSlices(String dbName, String tableName) {

        return submit(handle -> handle.createQuery("Select `SLICE`.* from " +
                "`DB` join `TBL` on `DB`.`id`=`TBL`.`dbId` join SLICE on `TBL`.`id`=`SLICE`.`tableId` " +
                "where `DB`.`name`=:dbName And `TBL`.`name`=:tableName and `SLICE`.`status`=:sliceStatus")
                .bind("dbName", dbName)
                .bind("tableName", tableName)
                .bind("sliceStatus", SliceStatus.READY)
                .mapToBean(Slice.class)
                .list());
    }

    //TODO use a more efficient fashion
    public List<Slice> getSlices(String dbName, String tableName, RangeSet<LocalDateTime> range) {
        List<Slice> sliceList = getSlices(dbName, tableName);
        List<Slice> ret = sliceList.stream()
                .filter(slice -> range.encloses(RangeUtil.of(slice.getStartTime(), slice.getEndTime())))
                .collect(Collectors.toList());
        return ret;
    }

    public List<Slice> getIntersectionSlices(String dbName, String tableName, RangeSet<LocalDateTime> range) {
        List<Slice> sliceList = getSlices(dbName, tableName);
        List<Slice> ret = sliceList.stream()
                .filter(slice -> range.intersects(RangeUtil.of(slice.getStartTime(), slice.getEndTime())))
                .collect(Collectors.toList());
        return ret;
    }

    /**
     * get slices which greater than the value
     */
    @Deprecated
    public List<Slice> getSlicesGT(String dbName, String tableName, LocalDateTime time) {

        return submit(handle -> handle.createQuery("Select `SLICE`.* from " +
                "`DB` join `TBL` on `DB`.`id`=`TBL`.`dbId` join `SLICE` on `TBL`.`id`=`SLICE`.`tableId` " +
                "where `DB`.`name`=:dbName And `TBL`.`name`=:tableName and `SLICE`.`status`=:sliceStatus and " +
                "(`SLICE`.`startTime`>:time or `SLICE`.`endTime`<:time)")
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
    public List<Slice> getSlicesGE(String dbName, String tableName, LocalDateTime time) {
        return submit(handle -> handle.createQuery("Select `SLICE`.* from " +
                "`DB` join `TBL` on `DB`.`id`=`TBL`.`dbId` join `SLICE` on `TBL`.`id`=`SLICE`.`tableId` " +
                "where `DB`.`name`=:dbName And `TBL`.`name`=:tableName and `SLICE`.`status`=:sliceStatus and " +
                "(`SLICE`.`startTime`>=:time or `SLICE`.`endTime`>:time)")
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
    public List<Slice> getSlicesLT(String dbName, String tableName, LocalDateTime time) {

        return submit(handle -> handle.createQuery("Select `SLICE`.* from " +
                "`DB` join `TBL` on `DB`.`id`=`TBL`.`dbId` join `SLICE` on `TBL`.`id`=`SLICE`.`tableId` " +
                "where `DB`.`name`=:dbName And `TBL`.`name`=:tableName and `SLICE`.`status`=:sliceStatus and" +
                " (`SLICE`.`endTime`<=:time)")
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
    public List<Slice> getSlicesLE(String dbName, String tableName, LocalDateTime time) {
        return submit(handle -> handle.createQuery("Select `SLICE`.* from " +
                "`DB` join `TBL` on `DB`.`id`=`TBL`.`dbId` join `SLICE` on `TBL`.`id`=`SLICE`.`tableId` " +
                "where `DB`.`name`=:dbName And `TBL`.`name`=:tableName and `SLICE`.`status`=:sliceStatus and" +
                " `SLICE`.`startTime`<=:time")
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
    public List<Slice> getSlicesBetween(String dbName, String tableName, LocalDateTime lower, LocalDateTime upper) {
        LocalDateTime nextUpperDate = upper.plusDays(1);

        return submit(handle -> handle.createQuery("Select `SLICE`.* from " +
                "`DB` join `TBL` on `DB`.`id`=`TBL`.`dbId` join `SLICE` on `TBL`.`id`=`SLICE`.`tableId` " +
                "where `DB`.`name`=:dbName And `TBL`.`name`=:tableName and `SLICE`.`status`=:sliceStatus and" +
                " `SLICE`.`startTime`>=:lower and `SLICE`.`endTIme`<=:upper")
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
        return submit(handle -> handle.createQuery("Select * from `DB` where `DB`.`name`=:dbName")
                .bind("dbName", dbName)
                .mapToBean(DB.class)
                .one());
    }

    public ImmutableList<DB> listDBs() {
        List<DB> dbsList= submit(handle -> handle.createQuery("Select * from `DB`")
                        .mapToBean(DB.class)
                        .list());

        ImmutableList<DB> dbs = ImmutableList.copyOf(dbsList);

        return dbs;
    }

    public ImmutableList<Table> listTable(String dbName){

        List<Table> tables = submit(handle -> {

            // 1. get dbId
            int dbId = handle.createQuery("Select `id` from `DB` where `name`= :name")
                    .bind("name", dbName)
                    .mapTo(Integer.class)
                    .one();

            List<Table> tableList = handle.createQuery("select TBL.* from TBL where `dbId`=:dbId")
                    .bind("dbId", dbId)
                    .mapToBean(Table.class)
                    .list();

            return tableList;
        });

        return ImmutableList.copyOf(tables);
    }

    /*-------------Metadata API for Job module--------------*/

    public void addJob(Job job) {
        submit(handle -> {
            String sql = "INSERT INTO `JOB`(`id`, `name`, `dbName`, `tableName`, `startTime`, `endTime`, `dataLowerTime`, `dataUpperTime`, `latestTask`, `state`, `type`) " +
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

    public void deleteJob(String jobId) {
        submit(handle -> {
            return handle.inTransaction(transaction -> {
                String sqlDeleteJob = String.format("DELETE FROM `JOB` where `id`='%s'", jobId);
                String sqlDeleteTask = String.format("DELETE FROM `TASK` where `jobId`='%s'", jobId);
                Batch batch = transaction.createBatch();
                batch.add(sqlDeleteJob);
                batch.add(sqlDeleteTask);
                return batch.execute();
            });
        });
    }

    public void completeJob(String jobId, JobState state, LocalDateTime endTime) {
        submit(handle -> {
            String sql = "Update `JOB` set `state`=? , `endTime`=? where `id`=?";
            return handle.execute(sql, state, endTime, jobId);
        });
    }

    public void updateJob(Job job) {
        submit(handle -> {
            String sql = "Update `JOB` set  `name`=?, `dbName`=?, `tableName`=?, `startTime`=?, `endTime`=?, " +
                    "`dataLowerTime`=?, `dataUpperTime`=?, `latestTask`=?, `state`=?, `type`=? where `id`=?";

            return handle.execute(sql,
                    job.getName(),
                    job.getDbName(),
                    job.getTableName(),
                    job.getStartTime(),
                    job.getEndTime(),
                    job.getDataLowerTime(),
                    job.getDataUpperTime(),
                    job.getLatestTask(),
                    job.getState(),
                    job.getType(),
                    job.getId());
        });
    }

    public void updateJobStatus(String jobId, JobState state) {
        submit(handle -> {
            String sql = "Update `JOB` set `state`=? where `id`=?";

            return handle.execute(sql, state, jobId);
        });
    }

    public void updateJobStartTime(String id, LocalDateTime startTime) {

        submit(handle -> {
            String sql = "Update `JOB` set `startTime`=? where `id` =?";

            return handle.execute(sql, startTime, id);
        });
    }

    public void updateJobEndTime(String id, LocalDateTime endTime) {

        submit(handle -> {
            String sql = "Update `JOB` set `endTime`=? where `id` =?";

            return handle.execute(sql, endTime, id);
        });
    }

    public Job getJob(String jobId) {
        return submit(handle -> {
//            try{
            Job job = handle.createQuery("Select `JOB`.* from `JOB` where `JOB`.`id`=:jobId")
                    .bind("jobId", jobId)
                    .mapToBean(Job.class)
                    .one();

            List<Task> tasks = handle.createQuery("select * from `TASK` where `jobId`=:jobId order by `step`")
                    .bind("jobId", jobId)
                    .mapToBean(Task.class)
                    .list();

            job.setTasks(tasks);
            return job;
//            }catch (IllegalStateException e){
//                return null;
//            }

        });

    }

    public Job getRunningDictJob() {

        return submit((handle -> {
            Job job = handle.createQuery("Select `JOB`.* from `JOB` where `JOB`.`state`=:state and `type`=:type")
                    .bind("state", JobState.RUNNING)
                    .bind("type", JobType.DICTIONARY)
                    .mapToBean(Job.class)
                    .one();
            return job;
        }));
    }

    public List<Job> listPendingJobs() {

        return submit((handle -> {
            List<Job> jobs = handle.createQuery("Select `JOB`.* from `JOB` where `JOB`.`state`=:state")
                    .bind("state", JobState.PENDING)
                    .mapToBean(Job.class)
                    .list();

            return jobs;
        }));
    }

    public List<Job> listNotCompletedJob(String dbName, String tableName, LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {
        //TODO TEST
        return submit(handle -> {

            List<Job> jobs = handle.createQuery("Select * from `JOB` where `JOB`.`dbName`=:dbName and `JOB`.`tableName`=:tableName " +
                    "and  `JOB`.`dataLowerTime`>=:dataLowerTime and  `JOB`.`dataUpperTime`<=:dataUpperTime " +
                    "and (`JOB`.`stateSuccess` != :state or `JOB`.`state` != :stateDiscard" )
                    .bind("dbName", dbName)
                    .bind("tableName", tableName)
                    .bind("dataLowerTime", dataLowerTime)
                    .bind("dataUpperTime", dataUpperTime)
                    .bind("stateSuccess", JobState.SUCCESS)
                    .bind("stateDiscard", JobState.DISCARD)
                    .mapToBean(Job.class)
                    .list();
            return jobs;
        });
    }

    public List<Job> listSuccessDictJobs(LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {

        return submit(handle -> {
            List<Job> jobs= handle.createQuery("Select * from `JOB` where `JOB`.`dataLowerTime`>=:dataLowerTime " +
                    "and  `JOB`.`dataUpperTime`<=:dataUpperTime ")
                    .bind("dataLowerTime", dataLowerTime)
                    .bind("dataUpperTime", dataUpperTime)
                    .mapToBean(Job.class)
                    .list();

            return jobs;
        });
    }

    public void addTask(Task task) {
        submit(handle -> {
            String sql = "INSERT INTO `TASK`(`id`, `name`, `jobId`, `appId`, `startTime`, `endTime`, `step`, `state`, `output`) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
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

    public Task getTask(String taskId) {
        return (submit(handle -> {
            Task task = handle.createQuery("Select `TASK`.* from `TASK` where `TASK`.`id`=:taskId")
                    .bind("taskId", taskId)
                    .mapToBean(Task.class)
                    .one();
            return task;
        }));
    }

    public Task getTask(String jobId, byte step) {

        return (submit(handle -> {
            Task task = handle.createQuery("Select `TASK`.* from `TASK` where `TASK`.`jobId`=:jobId and `step`=:step")
                    .bind("jobId", jobId)
                    .bind("step", step)
                    .mapToBean(Task.class)
                    .one();
            return task;
        }));
    }
    public void updateTaskStatus(String id, TaskState state) {
        submit(handle -> {
            String sql = "Update `TASK` set `state`=? where `id`=?";

            return handle.execute(sql, state, id);
        });
    }

    public void updateTaskAppId(String id, String appId) {
        submit(handle -> {
            String sql = "Update `TASK` set `appId`=? where `id`=?";

            return handle.execute(sql, appId, id);
        });
    }

    public void updateTaskStartTime(String id, LocalDateTime startTime) {

        submit(handle -> {
            String sql = "Update `TASK` set `startTime`=? where `id`=?";

            return handle.execute(sql, startTime, id);
        });
    }

    public void updateTaskEndTime(String id, LocalDateTime endTime) {

        submit(handle -> {
            String sql = "Update `TASK` set `endTime`=? where `id`=?";

            return handle.execute(sql, endTime, id);
        });
    }

    public void updateTaskOutput(String id, String output) {

        submit(handle -> {
            String sql = "Update `TASK` set `output`=? where `id`=?";

            return handle.execute(sql, output, id);
        });
    }

    public void completeTask(String taskId, TaskState state, LocalDateTime endTime, String output) {
        submit(handle -> {
            String sql = "Update `TASK` set `state`=? , `endTime`=? , `output`=? where `id`=?";
            return handle.execute(sql, state, endTime, output, taskId);
        });
    }

    public void updateTask(Task task) {
        submit(handle -> {

            String sql = "Update `TASK` set `name`=?, `jobId`=?, `appId`=?, `startTime`=?, " +
                    "`endTime`=?, `step`=?, `state`=?, `output`=? where `id`=?";
            return handle.execute(sql,
                    task.getName(),
                    task.getJobId(),
                    task.getAppId(),
                    task.getStartTime(),
                    task.getEndTime(),
                    task.getStep(),
                    task.getState(),
                    task.getOutput(),
                    task.getId());
        });
    }


    /*-------------Metadata API for Dict module--------------*/

    //TODO put two sql in transaction
    public void addDict(Dict dict) {

        submit(handle -> {

            if (DictStatus.READY != dict.getStatus()) {
                throw new MetadataException(MetadataErrorCode.METADATA_ERROR, "Newly added dict must be READY status.");
            }

            String sqlDisableDict = "UPDATE `DICT` set `status`=? where `status`=?";
            handle.execute(sqlDisableDict, DictStatus.UNUSED, DictStatus.READY);

            String sqlAddDict = "INSERT INTO `DICT`(`version`, `status`, `location`, `elementCount`, `createDate`, `type`) " +
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
        return submit(handle -> handle.createQuery("SELECT * from `DICT` where `status`=:status")
                .bind("status", DictStatus.READY)
                .mapToBean(Dict.class)
                .one());
    }

    public long getDictElementCount() {
        return submit(handle -> handle.createQuery("SELECT `elementCount` from `DICT` where `status`=:status")
                .bind("status", DictStatus.READY)
                .mapTo(Long.class)
                .one());
    }



}
