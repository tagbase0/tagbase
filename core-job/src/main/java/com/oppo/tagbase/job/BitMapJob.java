package com.oppo.tagbase.job;

import com.oppo.tagbase.job.util.DateFormat;
import com.oppo.tagbase.job.util.IdGenerator;
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
    public String buildDict(String dbName, String tableName) {
        throw new UnsupportedOperationException("Data build job  doesn't support build dictionary !");
    }

    @Override
    public String buildData(String dbName, String tableName, String lowerDate, String upperDate) {
        Job job = iniJob(dbName, tableName, lowerDate, upperDate);
        return build(job);
    }

    @Override
    public Job jobInfo(String jobId) {
        return new MetadataJob().getJob(jobId);
    }


    public Job iniJob(String dbName, String tableName, String lowerDate, String upperDate) {
        Job bitMapJob = new Job();

        Date dataLowerDate = new DateFormat().toDate(dbName, tableName, lowerDate);
        Date dataUpperDate = new DateFormat().toDate(dbName, tableName, upperDate);

        String bitMapJobId = new IdGenerator().nextQueryId("BitMapBuildJob");
        String jobName = dbName + "_" + tableName + "_" + dataLowerDate + "_" + dataUpperDate;

        bitMapJob.setId(bitMapJobId);
        bitMapJob.setName(jobName);
        bitMapJob.setDbName(dbName);
        bitMapJob.setTableName(tableName);
        bitMapJob.setStartTime(new Date(System.currentTimeMillis()));
        bitMapJob.setDataLowerTime(dataLowerDate);
        bitMapJob.setDataUpperTime(dataUpperDate);
        bitMapJob.setLatestTask("");
        bitMapJob.setState(JobState.PENDING);
        bitMapJob.setType(JobType.DATA);


        new MetadataJob().addJob(bitMapJob);

        // 定义子任务 tasks

        return bitMapJob;
    }


    public String build(Job bitMapJob) {

        // 将此 job 放到 pending 队列
        PENDING_JOBS_QUEUE.offer(bitMapJob);
        log.info("{} is pending", bitMapJob.getId());

        // 若已准备好 构建
        while (readytoBuild()) {

            Job bitMapJobHead = AbstractJob.PENDING_JOBS_QUEUE.peek();

            Future<Slice> slice = JOB_EXECUTORS.submit(new BitMapBuildJob(bitMapJobHead.getId()));

        }

        return bitMapJob.getId();

    }

    public boolean readytoBuild() {
        // 若反向字典已经构建完成，且当前负载不高时，从pending队列取一个到running队列
        return succeed(DictJob.DICT_JOB_ID) && AbstractJob.RUNNING_JOBS_QUEUE.size() <= RUNNING_JOBS_LIMIT;
    }

}
