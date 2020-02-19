package com.oppo.tagbase.job;

import com.oppo.tagbase.job.util.IdGenerator;
import com.oppo.tagbase.job.util.TaskHelper;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.List;


/**
 * Created by daikai on 2020/2/16.
 */
public class DictJob implements AbstractJob {

    protected static String DICT_JOB_ID;

    Logger log = LoggerFactory.getLogger(DictJob.class);

    @Override
    public boolean succeed(String jobId) {
        if (JobState.SUCCESS == new MetadataJob().getJob(jobId).getState()) {
            return true;
        }
        return false;
    }

    @Override
    public void buildDict(String dbName, String tableName) {
        Job job = iniJob(dbName, tableName);
        build(job);


        // 更新元数据模块内容
        new MetadataJob().completeJOb(job.getId(), JobState.SUCCESS, new Date(System.currentTimeMillis()));
    }

    @Override
    public String buildData(String dbName, String tableName, String lowerDate, String upperDate) {
        throw new UnsupportedOperationException("Dict build job  doesn't support build data !");
    }

    @Override
    public Job jobInfo(String jobId) {
        return new MetadataJob().getJob(jobId);
    }


    public Job iniJob(String dbName, String tableName) {

        Job dictJob = new Job();

        DICT_JOB_ID = new IdGenerator().nextQueryId("DictBuildJob", "yyyyMMdd");
        String today = new SimpleDateFormat("yyyyMMdd").format(System.currentTimeMillis());

        dictJob.setId(DICT_JOB_ID);
        dictJob.setName(dbName + "_" + tableName + "_" + today);
        dictJob.setDbName(dbName);
        dictJob.setTableName(tableName);
        dictJob.setStartTime(new Date(System.currentTimeMillis()));
        dictJob.setLatestTask("");
        dictJob.setState(JobState.PENDING);
        dictJob.setType(JobType.DICTIONARY);

        new MetadataJob().addJob(dictJob);

        // 定义两个task，分别构建反向字典和正向字典
        Task invertedTask = new Task();
        Task forwardTask = new Task();

        invertedTask.setId(new IdGenerator().nextQueryId("InvertedDictBuildTask", "yyyyMMdd"));
        invertedTask.setName("InvertedDictBuildTask_"+today);
        invertedTask.setJobId(DICT_JOB_ID);
        invertedTask.setStep((byte)0);
        String outputInverted = "";
        invertedTask.setOutput(outputInverted);


        forwardTask.setId(new IdGenerator().nextQueryId("ForwardDictBuildTask", "yyyyMMdd"));
        forwardTask.setName("ForwardDictBuildTask_"+today);
        forwardTask.setJobId(DICT_JOB_ID);
        forwardTask.setStep((byte)1);
        String outputForward = "";
        forwardTask.setOutput(outputForward);

        new MetadataJob().addTask(invertedTask);
        new MetadataJob().addTask(forwardTask);

        return dictJob;

    }

    public String build(Job dictJob) {

        // 将此 job 放到 pending 队列
//        PENDING_JOBS_QUEUE.offer(dictJob);
        log.info("{} is pending", DICT_JOB_ID);

        // 若已准备好构建
        if (readytoBuild()) {

            buildDict(dictJob);
            dictJob.setEndTime(new Date(System.currentTimeMillis()));
            dictJob.setState(JobState.SUCCESS);
            log.info("{} is finished", DICT_JOB_ID);
        } else {

            log.info("Skip to build dictionary !");
        }

        return DICT_JOB_ID;
    }

    private boolean readytoBuild() {
        return true;
    }

    public void buildDict(Job dictJob) {
        dictJob.setState(JobState.RUNNING);
        log.info("{} is running", DICT_JOB_ID);
        //TODO 2020/2/16  build dictionary

        List<Task> tasks = dictJob.getTasks();
        int stepNum = tasks.size();

        JobState jobState = dictJob.getState();
        //TODO 2020/2/16  分步骤执行
        // 当该 job 下所有子任务都执行成功，则循环结束
        for (int i = 0; jobState != JobState.SUCCESS; i++) {
            switch (i) {
                case 0:
                    // 仅仅当前任务的前置任务都正常执行成功，才开启这个任务
                    if (new TaskHelper().preTaskFinish(tasks, i)) {
                        // InvertedDictTask;
                        // 参数：task, dbName, tableName

                        Task task = tasks.get(0);
                        new MetadataJob().completeTask(task.getId(),
                                task.getState(),
                                new Date(System.currentTimeMillis()),
                                task.getOutput());
                    }
                    break;
                case 1:
                    if (new TaskHelper().preTaskFinish(tasks, i)) {
                        // ForwardDictTask;
                        Task task = tasks.get(1);
                        String locationInverted = tasks.get(0).getOutput();
                        // 参数 locationInverted, task


                        String locationForwardNew = "";
                        task.setOutput(locationForwardNew);
                        new MetadataJob().completeTask(task.getId(),
                                task.getState(),
                                new Date(System.currentTimeMillis()),
                                task.getOutput());
                    }
                    break;
                default:
                    break;

            }
            i = i % stepNum;
        }

        new MetadataJob().completeJOb(dictJob.getId(), JobState.SUCCESS, new Date(System.currentTimeMillis()));

    }




}
