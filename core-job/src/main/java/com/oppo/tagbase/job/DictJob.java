package com.oppo.tagbase.job;

import com.google.inject.Injector;
import com.oppo.tagbase.job.engine.obj.HiveMeta;
import com.oppo.tagbase.job.engine.obj.TaskMessage;
import com.oppo.tagbase.job.util.IdGenerator;
import com.oppo.tagbase.job.util.TableHelper;
import com.oppo.tagbase.job.util.TaskHelper;
import com.oppo.tagbase.meta.MetadataDict;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Dict;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobState;
import com.oppo.tagbase.meta.obj.JobType;
import com.oppo.tagbase.meta.obj.Task;
import com.oppo.tagbase.meta.obj.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * Created by daikai on 2020/2/16.
 */
public class DictJob implements AbstractJob {


    private static String INVERTED_DICT_HDFS_PATH_PRE = "/user/tagbase/dict/inverted/";
    private static String FORWARD_DICT_HDFS_PATH_PRE = "/user/tagbase/dict/forward/";
    private static Long DICT_TASK_RETRY_TIME_MS = 5000L;

    private Logger log = LoggerFactory.getLogger(DictJob.class);

    private Injector injector;

    @Override
    public void buildDict(String dbName, String tableName) {

        Job job = iniJob(dbName, tableName);

        build(job);

        // update MetadataJob job info
        new MetadataJob().completeJob(job.getId(), job.getState(), LocalDateTime.now());

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

        // like DictBuildJob_20200222
        String dictJobId = new IdGenerator().nextQueryId("DictBuildJob", "yyyyMMdd");
        log.debug("{} is initializing.", dictJobId);

        dictJob.setId(dictJobId);
        dictJob.setName(dbName + "_" + tableName + "_" + dictJobId);
        dictJob.setDbName(dbName);
        dictJob.setTableName(tableName);
        dictJob.setStartTime(LocalDateTime.now());
        dictJob.setState(JobState.PENDING);
        dictJob.setType(JobType.DICTIONARY);

        // initialize  tasks
        iniTasks(dictJob);
        new MetadataJob().addJob(dictJob);
        log.debug("{} completes initialization.", dictJobId);

        return dictJob;

    }

    private void iniTasks(Job dictJob) {

        // initialize invertedDict task
        Task invertedTask = new Task();
        String date = new Date(System.currentTimeMillis()).toString().replace("-", "");

        String outputInvertedPath = INVERTED_DICT_HDFS_PATH_PRE + date;
        iniTask(dictJob.getId(), invertedTask, "InvertedDictBuildTask", (byte) 0, outputInvertedPath);

        // initialize forwardTask task
        Task forwardTask = new Task();
        String outputForwardPath = FORWARD_DICT_HDFS_PATH_PRE + date;
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

        // if ready, build; or wait a moment and retry
        if (readyToBuild()) {

            buildDict(dictJob);

        } else {
            log.info("Not the right time to build Dictionary job {}!", jobId);
            try {
                TimeUnit.MILLISECONDS.sleep(DICT_TASK_RETRY_TIME_MS);
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


        // if all the tasks of the job finished, jump out of the loop
        for (int i = 0; dictJob.getState() != JobState.SUCCESS; i++) {
            switch (i) {
                case 0:
                    // task is started only if the previous task has been executed successfully
                    if (new TaskHelper().preTaskFinish(tasks, i)) {
                        // InvertedDictTask;
                        Task task = tasks.get(i);

                        doInvertedDictTask(task, dictJob);

                        log.debug("InvertedDictTask {} is finished.", task.getId());
                    }
                    break;
                case 1:
                    if (new TaskHelper().preTaskFinish(tasks, i)) {

                        Task task = tasks.get(i);
                        String invertedDictHDFSPath = tasks.get(0).getOutput();
                        doForwardDictTask(task, dictJob, invertedDictHDFSPath);

                        dictJob.setState(JobState.SUCCESS);
                        log.debug("ForwardDictTask {} is finished.", task.getId());
                    }
                    break;
                default:
                    break;

            }
            i = i % stepNum;
        }

    }

    private void doForwardDictTask(Task task, Job dictJob, String invertedDictHDFSPath) {

        // 1. update Metadata, construct the input of buildDictForward
        dictJob.setLatestTask(task.getId());
        task.setState(TaskState.RUNNING);

        Dict dictForwardOld = new MetadataDict().getDict();

        // 2. do task
        // construct the latest forward dictionary
        Dict dictForwardToday = new TaskHelper().buildDictForward(invertedDictHDFSPath,
                dictForwardOld, task.getOutput());

        // 3. update Metadata and the forward dictionary
        String fileLocationForwardNew = dictForwardToday.getLocation();
        task.setOutput(fileLocationForwardNew);
        new MetadataJob().completeTask(task.getId(),
                task.getState(),
                LocalDateTime.now(),
                task.getOutput());

        new MetadataDict().addDict(dictForwardToday);
    }

    private void doInvertedDictTask(Task task, Job dictJob) {

        // 1. update Metadata, construct the HiveMeta object
        dictJob.setLatestTask(task.getId());
        task.setState(TaskState.RUNNING);

        HiveMeta hiveMeta = new TableHelper().generalHiveMeta(task, dictJob);

        log.debug("InvertedDictTask {} start.", task.getId());

        // 2. start the engine task
        TaskEngine sparkTaskEngine = injector.getInstance(TaskEngine.class);
        String appId = null;
        TaskMessage taskMessage = null;
        TaskState state;

        try {
            appId = sparkTaskEngine.submitTask(hiveMeta, JobType.DICTIONARY);
            taskMessage = sparkTaskEngine.getTaskStatus(appId, JobType.DICTIONARY);

        } catch (Exception e) {
            log.error("{}, Error to run TaskEngine!", task.getId());
        }

        task.setAppId(appId);
        state = taskMessage.parseJobStatus();

        // 3. update Metadata
        new MetadataJob().completeTask(task.getId(),
                state,
                LocalDateTime.now(),
                task.getOutput());
    }

}
