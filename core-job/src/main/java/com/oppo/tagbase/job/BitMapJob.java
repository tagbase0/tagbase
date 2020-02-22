package com.oppo.tagbase.job;

import com.oppo.tagbase.job.util.DateFormat;
import com.oppo.tagbase.job.util.IdGenerator;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.concurrent.Future;

/**
 * Created by daikai on 2020/2/16.
 */
public class BitMapJob implements AbstractJob {

    Logger log = LoggerFactory.getLogger(BitMapJob.class);
    private static final int RUNNING_JOBS_LIMIT = 50;


    @Override
    public void buildDict(String dbName, String tableName) {
        throw new UnsupportedOperationException("Data build job  doesn't support build dictionary !");
    }

    @Override
    public String buildData(String dbName, String tableName, String lowerDate, String upperDate) {

        Job job = iniJob(dbName, tableName, lowerDate, upperDate);

        String jobId = build(job);

        // 更新元数据模块内容
        new MetadataJob().completeJOb(jobId, job.getState(), new Date(System.currentTimeMillis()));
        log.info("{} is finished.", job.getId());

        return jobId;
    }

    @Override
    public Job jobState(String jobId) {
        return new MetadataJob().getJob(jobId);
    }


    public Job iniJob(String dbName, String tableName, String lowerDate, String upperDate) {

        Job bitMapJob = new Job();

        Date dataLowerDate = new DateFormat().toDate(dbName, tableName, lowerDate);
        Date dataUpperDate = new DateFormat().toDate(dbName, tableName, upperDate);

        String bitMapJobId = new IdGenerator().nextQueryId("DataBuildJob");
        String jobName = dbName + "_" + tableName + "_" + dataLowerDate + "_" + dataUpperDate;

        log.debug("{} is initializing.", bitMapJobId);

        bitMapJob.setId(bitMapJobId);
        bitMapJob.setName(jobName);
        bitMapJob.setDbName(dbName);
        bitMapJob.setTableName(tableName);
        bitMapJob.setStartTime(new Date(System.currentTimeMillis()));
        bitMapJob.setDataLowerTime(dataLowerDate);
        bitMapJob.setDataUpperTime(dataUpperDate);
        bitMapJob.setState(JobState.PENDING);
        bitMapJob.setType(JobType.DATA);

        // 定义子任务 tasks
        iniTasks(bitMapJob);

        new MetadataJob().addJob(bitMapJob);
        log.debug("{} completes initialization.", bitMapJobId);

        return bitMapJob;
    }


    private void iniTasks(Job bitMapJob) {

        // task1 for Hfile
        Task hFileTask = new Task();
        String outputHfile = "";
        iniTask(bitMapJob.getId(), hFileTask, "DataBuildHfileTask", (byte) 0, outputHfile);

        // task2 for bulkload
        Task loadTask = new Task();
        String outputLoad = "";
        iniTask(bitMapJob.getId(), loadTask, "DataBuildLoadTask", (byte) 1, outputLoad);

        new MetadataJob().addTask(hFileTask);
        new MetadataJob().addTask(loadTask);

        log.debug("{} completes initialization.", hFileTask.getId());
        log.debug("{} completes initialization.", loadTask.getId());
    }

    private void iniTask(String jobId, Task task, String name, byte step, String output) {

        String today = new SimpleDateFormat("yyyyMMdd").format(System.currentTimeMillis());
        String taskId = new IdGenerator().nextQueryId(name, "yyyyMMdd");
        log.debug("{} is initializing. ", taskId);
        task.setId(taskId);
        task.setName(name + "_" + today);
        task.setJobId(jobId);
        task.setStep(step);
        task.setOutput(output);
        task.setState(TaskState.PENDING);

    }

    public String build(Job bitMapJob) {

        // 将此 job 放到 pending 队列
        PENDING_JOBS_QUEUE.offer(bitMapJob);
        log.info("{} is pending", bitMapJob.getId());

        // 若已准备好 构建
        while (readytoBuild()) {

            Job bitMapJobHead = AbstractJob.PENDING_JOBS_QUEUE.peek();

            Future<Slice> slice = JOB_EXECUTORS.submit(new BitMapBuildJob(bitMapJobHead.getId()));

            new MetadataJob().completeJOb(bitMapJobHead.getId(), bitMapJobHead.getState(),
                    new Date(System.currentTimeMillis()));

            log.info("{} is finished.", bitMapJobHead.getId());
        }

        return bitMapJob.getId();

    }

    public boolean readytoBuild() {
        // 若反向字典已经构建完成，且当前负载不高时，从pending队列取一个到running队列
        return invertedDictSucceed(new IdGenerator().nextQueryId("DictBuildJob", "yyyyMMdd")) &&
                AbstractJob.RUNNING_JOBS_QUEUE.size() <= RUNNING_JOBS_LIMIT;
    }

    public boolean invertedDictSucceed(String jobId) {
        if (TaskState.SUCCESS == new MetadataJob().getJob(jobId).getTasks().get(0).getState()) {
            return true;
        }
        return false;
    }

}
