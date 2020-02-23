package com.oppo.tagbase.job;

import com.google.inject.Injector;
import com.oppo.tagbase.job.obj.HiveMeta;
import com.oppo.tagbase.job.util.IdGenerator;
import com.oppo.tagbase.job.util.TableHelper;
import com.oppo.tagbase.job.util.TaskHelper;
import com.oppo.tagbase.meta.MetadataDict;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Created by daikai on 2020/2/16.
 */
public class DictJob implements AbstractJob {

    Logger log = LoggerFactory.getLogger(DictJob.class);

    private static Injector injector;

    @Override
    public void buildDict(String dbName, String tableName) {

        Job job = iniJob(dbName, tableName);

        build(job);

        // 更新 Job 元数据模块内容
        new MetadataJob().completeJOb(job.getId(), job.getState(), new Date(System.currentTimeMillis()));

        log.info("{} is finished.", job.getId());

    }

    @Override
    public String buildData(String dbName, String tableName, String lowerDate, String upperDate) {
        throw new UnsupportedOperationException("Dict build job  doesn't support build data !");
    }

    @Override
    public Job jobState(String jobId) {
        return new MetadataJob().getJob(jobId);
    }


    public Job iniJob(String dbName, String tableName) {

        Job dictJob = new Job();

        // DictBuildJob_20200222
        String dictJobId = new IdGenerator().nextQueryId("DictBuildJob", "yyyyMMdd");
        log.debug("{} is initializing.", dictJobId);

        dictJob.setId(dictJobId);
        dictJob.setName(dbName + "_" + tableName + "_" + dictJobId);
        dictJob.setDbName(dbName);
        dictJob.setTableName(tableName);
        dictJob.setStartTime(new Date(System.currentTimeMillis()));
        dictJob.setState(JobState.PENDING);
        dictJob.setType(JobType.DICTIONARY);

        // 定义子任务 tasks
        iniTasks(dictJob);
        new MetadataJob().addJob(dictJob);
        log.debug("{} completes initialization.", dictJobId);

        return dictJob;

    }

    private void iniTasks(Job dictJob) {

        // task invertedTask 初始化反向字典
        Task invertedTask = new Task();
        //TODO 2020/2/20  任务输出路径待指定
        String outputInvertedPath = "";
        iniTask(dictJob.getId(), invertedTask, "InvertedDictBuildTask", (byte) 0, outputInvertedPath);

        // task forwardTask 初始化正向字典
        Task forwardTask = new Task();
        String outputForwardPath = "";
        iniTask(dictJob.getId(), forwardTask, "ForwardDictBuildTask", (byte) 1, outputForwardPath);

        new MetadataJob().addTask(invertedTask);
        new MetadataJob().addTask(forwardTask);

        log.debug("{} completes initialization.", invertedTask.getId());
        log.debug("{} completes initialization.", forwardTask.getId());
    }

    private void iniTask(String jobId, Task task, String name, byte step, String output) {

        String taskId = new IdGenerator().nextQueryId(name, "yyyyMMdd");
        log.debug("{} is initializing. ", taskId);

        task.setId(taskId);
        task.setName(taskId);
        task.setJobId(jobId);
        task.setStep(step);
        task.setOutput(output);
        task.setState(TaskState.PENDING);
    }

    public void build(Job dictJob) {

        String jobId = dictJob.getId();
        log.info("Dictionary job {} is pending.", jobId);

        // 若已准备好 则构建; 否则稍后再构建
        if (readyToBuild()) {

            buildDict(dictJob);

        } else {
            log.info("Not the right time to build Dictionary job {}!", jobId);
            try {
                TimeUnit.MILLISECONDS.sleep(5000);
            } catch (InterruptedException e) {
                log.error("{} Error when to sleep!", jobId);
            }

            build(dictJob);
        }

    }

    private boolean readyToBuild() {

        String jobIdToday = new IdGenerator().nextQueryId("DictBuildJob", "yyyyMMdd");

        if (JobState.RUNNING != new MetadataJob().getJob(jobIdToday).getState() &&
                JobState.PENDING != new MetadataJob().getJob(jobIdToday).getState()) {

            return true;
        }
        return false;
    }

    public void buildDict(Job dictJob) {

        dictJob.setState(JobState.RUNNING);
        log.info("Dictionary job {} is running.", dictJob.getId());

        List<Task> tasks = dictJob.getTasks();
        int stepNum = tasks.size();


        // 当该 job 下所有子任务都执行成功，则循环结束
        for (int i = 0; dictJob.getState() != JobState.SUCCESS; i++) {
            switch (i) {
                case 0:
                    // 仅仅当前任务的前置任务都正常执行成功，才开启这个任务
                    if (new TaskHelper().preTaskFinish(tasks, i)) {
                        // InvertedDictTask;
                        // 1. 元数据状态变更, 构造 HiveMeta 对象
                        Task task = tasks.get(i);
                        dictJob.setLatestTask(task.getId());
                        task.setState(TaskState.RUNNING);

                        HiveMeta hiveMeta = new TableHelper().generalHiveMeta(task, dictJob);

                        log.debug("InvertedDictTask {} start.", task.getId());

                        // 2. 启动任务
                        //TODO 2020/2/16  调用反向字典Spark任务
                        TaskEngine sparkTaskEngine = injector.getInstance(TaskEngine.class);
                        String appId = null;
                        String finalStatus = null;
                        TaskState state = null;

                        try {
                            appId = sparkTaskEngine.submitTask(hiveMeta, JobType.DICTIONARY);
                            finalStatus = sparkTaskEngine.getTaskStatus(appId, JobType.DICTIONARY).getFinalStatus();

                        } catch (Exception e) {
                            log.error("{}, Error to run TaskEngine!", task.getId());
                        }

                        task.setAppId(appId);

                        //TODO convert finalStatus to TaskState

                        // 3.更新任务状态信息
                        new MetadataJob().completeTask(task.getId(),
                                state,
                                new Date(System.currentTimeMillis()),
                                task.getOutput());

                        log.debug("InvertedDictTask {} is finished.", task.getId());
                    }
                    break;
                case 1:
                    if (new TaskHelper().preTaskFinish(tasks, i)) {

                        // 1. 元数据状态变更, task输入参数获取
                        Task task = tasks.get(i);
                        dictJob.setLatestTask(task.getId());
                        task.setState(TaskState.RUNNING);
                        String invertedDictHDFSPath = tasks.get(0).getOutput();
                        Dict dictForwardOld = new MetadataDict().getDict();

                        // 2. 构造最新的正向字典, 更新正向字典相关文件
                        // 参数 invertedDictPath, dictForwardOld, 以及正向字典保存的Hdfs目录task.getOutput()
                        Dict dictForwardToday = new TaskHelper().buildDictForward(invertedDictHDFSPath,
                                dictForwardOld, task.getOutput());

                        // 3. Task元数据更新
                        String fileLocationForwardNew = dictForwardToday.getLocation();
                        task.setOutput(fileLocationForwardNew);
                        new MetadataJob().completeTask(task.getId(),
                                task.getState(),
                                new Date(System.currentTimeMillis()),
                                task.getOutput());

                        new MetadataDict().addDict(dictForwardToday);

                        dictJob.setState(JobState.SUCCESS);

                        log.info("ForwardDictTask {} is finished.", task.getId());
                    }
                    break;
                default:
                    break;

            }
            i = i % stepNum;
        }

    }

}
