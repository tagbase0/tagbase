package com.oppo.tagbase.job;

import com.oppo.tagbase.job.util.DateFormat;
import com.oppo.tagbase.job.util.IdGenerator;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobState;
import com.oppo.tagbase.meta.obj.JobType;
import com.oppo.tagbase.meta.obj.Slice;
import com.oppo.tagbase.meta.obj.Task;
import com.oppo.tagbase.meta.obj.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
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

        // update MetadataJob job info
        new MetadataJob().completeJOb(jobId, job.getState(), LocalDateTime.now());
        log.info("{} is finished.", job.getId());

        return jobId;
    }

    @Override
    public Job jobState(String jobId) {
        return new MetadataJob().getJob(jobId);
    }


    public Job iniJob(String dbName, String tableName, String lowerDate, String upperDate) {

        Job bitMapJob = new Job();

        Date dataLowerDate = new DateFormat().toDate(lowerDate);
        Date dataUpperDate = new DateFormat().toDate(upperDate);

        String bitMapJobId = new IdGenerator().nextQueryId("DataBuildJob");
        String jobName = dbName + "_" + tableName + "_" + dataLowerDate + "_" + dataUpperDate;

        log.debug("{} is initializing.", bitMapJobId);

        bitMapJob.setId(bitMapJobId);
        bitMapJob.setName(jobName);
        bitMapJob.setDbName(dbName);
        bitMapJob.setTableName(tableName);
        bitMapJob.setStartTime(LocalDateTime.now());
        bitMapJob.setDataLowerTime(LocalDateTime.now());
        bitMapJob.setDataUpperTime(LocalDateTime.now());
        bitMapJob.setState(JobState.PENDING);
        bitMapJob.setType(JobType.DATA);

        // initialize  tasks
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

        // put the job to pending queue
        PENDING_JOBS_QUEUE.offer(bitMapJob);
        log.info("{} is pending", bitMapJob.getId());

        // if ready, build
        while (readytoBuild()) {

            Job bitMapJobHead = AbstractJob.PENDING_JOBS_QUEUE.peek();

            Future<Slice> slice = JOB_EXECUTORS.submit(new BitMapBuildJob(bitMapJobHead.getId()));

            new MetadataJob().completeJOb(bitMapJobHead.getId(), bitMapJobHead.getState(),
                    LocalDateTime.now());

            log.info("{} is finished.", bitMapJobHead.getId());
        }

        return bitMapJob.getId();

    }

    public boolean readytoBuild() {

        // if the invertedDict has been built, current load is ok, get a task from the pending queue
        return invertedDictSucceed(new IdGenerator().nextQueryId("DictBuildJob", "yyyyMMdd")) &&
                AbstractJob.RUNNING_JOBS_QUEUE.size() <= RUNNING_JOBS_LIMIT &&
                AbstractJob.RUNNING_JOBS_QUEUE.size() > 0;
    }

    public boolean invertedDictSucceed(String jobId) {
        if (TaskState.SUCCESS == new MetadataJob().getJob(jobId).getTasks().get(0).getState()) {
            return true;
        }
        return false;
    }

}
