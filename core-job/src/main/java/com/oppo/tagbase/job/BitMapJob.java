package com.oppo.tagbase.job;

import com.oppo.tagbase.job.util.IdGenerator;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.concurrent.Future;

/**
 * Created by daikai on 2020/2/16.
 */
public class BitMapJob implements AbstractJob {

    Logger log = LoggerFactory.getLogger(BitMapJob.class);
    private static final int RUNNING_JOBS_LIMIT = 50;

    @Override
    public boolean succeed(String jobId) {
        return false;
    }

    @Override
    public String build(String dbName, String tableName, String jobType) {
        if ("data".equalsIgnoreCase(jobType)) {
            return build(dbName, tableName);
        } else {
            return null;
        }
    }

    @Override
    public void iniJob(Job job) {
        new MetadataJob().createJob(job);

        // 定义子任务 tasks

    }


    public String build(String dbName, String tableName) {
        Table table = new Metadata().getTable(dbName, tableName);
        Job bitMapJob = new Job();

        String bitMapJobId = new IdGenerator().nextQueryId("BitMapBuildJob");
        String jobName = "BitMapBuildJob_" + table.getSrcDb() + "_" + table.getSrcTable() +
                table.getName() + "_" + table.getDesc();

        bitMapJob.setId(bitMapJobId);
        bitMapJob.setName(jobName);
        bitMapJob.setDbName(dbName);
        bitMapJob.setTableName(tableName);
        bitMapJob.setSliceName("");
        bitMapJob.setStartTime(new Date(System.currentTimeMillis()));
        bitMapJob.setLatestTask("");
        bitMapJob.setState(JobState.PENDING);
        bitMapJob.setType(JobType.DATA);

        iniJob(bitMapJob);

        // 将此 job 放到 pending 队列
        PENDING_JOBS_QUEUE.offer(bitMapJob);
        log.info("{} is pending", bitMapJobId);

        // 若已准备好 构建
        while (readytoBuild()) {

            Job bitMapJobHead = AbstractJob.PENDING_JOBS_QUEUE.peek();

            Future<Slice> slice = JOB_EXECUTORS.submit(new BitMapBuildJob(bitMapJobHead.getId()));

        }

        return bitMapJobId;

    }

    public boolean readytoBuild() {
        // 若反向字典已经构建完成，且当前负载不高时，从pending队列取一个到running队列
        return succeed(DictJob.DICT_JOB_ID) && AbstractJob.RUNNING_JOBS_QUEUE.size() <= RUNNING_JOBS_LIMIT;
    }

}
