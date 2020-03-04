package com.oppo.tagbase.meta;

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.oppo.tagbase.common.guice.PropsModule;
import com.oppo.tagbase.common.guice.ValidatorModule;
import com.oppo.tagbase.meta.connector.MetaStoreConnectorConfig;
import com.oppo.tagbase.meta.obj.*;
import com.oppo.tagbase.meta.util.DateUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import java.time.LocalDateTime;

import java.util.List;

import static java.time.LocalDateTime.now;

/**
 * Created by daikai on 2020/2/27.
 */
public class MetadataJobTest {


    MetadataJob metadataJob;
    Metadata metadata;
    Job job = new Job();
    Task task0 = new Task();
    Task task1 = new Task();

    @Before
    public void setup() {
        Injector injector = Guice.createInjector(
                new ValidatorModule(),
                new PropsModule(Lists.newArrayList("tagbase.properties")),
                new MetadataModule()
        );

        MetaStoreConnectorConfig c = injector.getInstance(MetaStoreConnectorConfig.class);

        metadataJob = injector.getInstance(MetadataJob.class);
        metadata = injector.getInstance(Metadata.class);

        iniJob();
        iniTask();

    }

    public void iniJob() {

        job.setId("DictBuild20200227");
        job.setName("DictBuild20200227_1");
        job.setDbName("hive");
        job.setTableName("gobal_imei");
        job.setStartTime(DateUtil.toLocalDateTime("2020-02-27 10:12:05"));
        job.setState(JobState.PENDING);
        job.setLatestTask("");
        job.setDataLowerTime(DateUtil.toLocalDateTime("2020-02-26 10:12:05"));
        job.setDataUpperTime(DateUtil.toLocalDateTime("2020-02-27 10:12:05"));
        job.setType(JobType.DICTIONARY);
        job.setCreateTime(DateUtil.toLocalDateTime("2020-02-27 09:12:05"));
        job.setProgress(0f);

    }


    public void iniTask() {

        task0.setId("TaskDictInvertedBuild20200227");
        task0.setName("TaskDictInvertedBuild20200227_1");
        task0.setState(TaskState.PENDING);
        task0.setOutput("/user/hive/tagbase/dict/inverted/20200227");
        task0.setStep((byte) 0);
        task0.setJobId("DictBuild20200227");
        task0.setStartTime(DateUtil.toLocalDateTime("2020-02-27 10:12:05"));

        task1.setId("TaskDictForwardBuild20200227");
        task1.setName("TaskDictForwardBuild20200227_1");
        task1.setState(TaskState.PENDING);
        task1.setOutput("/user/hive/tagbase/dict/forward/20200227");
        task1.setStep((byte) 1);
        task1.setJobId("DictBuild20200227");
        task1.setStartTime(DateUtil.toLocalDateTime("2020-02-27 10:12:05"));
    }

    /*------------ Start to test --------------*/

    public void addJob() {

        metadataJob.addJob(job);
        Assert.assertEquals(job, metadataJob.getJob(job.getId()));
        metadataJob.deleteJob(job.getId());
    }


    public void getJob() {
        metadataJob.addJob(job);
        Assert.assertEquals(job.getId(), metadataJob.getJob(job.getId()).getId());
        metadataJob.deleteJob(job.getId());
    }


    public void addTask() {

        metadataJob.addJob(job);
        metadataJob.addTask(task0);
        metadataJob.addTask(task1);

        Assert.assertEquals(task0, metadataJob.getTask(task0.getId()));
        Assert.assertEquals(task1, metadataJob.getTask(task1.getId()));

        metadataJob.deleteJob(job.getId());

    }


    public void getPendingJobCount() {

        metadataJob.addJob(job);
        Assert.assertEquals(1, metadataJob.getPendingJobCount());
        metadataJob.deleteJob(job.getId());
    }


    public void listPendingJobs() {

        metadataJob.addJob(job);
        List<Job> jobs = metadataJob.listPendingJobs();
        Assert.assertEquals(JobState.PENDING, jobs.get(0).getState());
        metadataJob.deleteJob(job.getId());
    }


    public void getLatestDictJob() {

        metadataJob.addJob(job);
        Job jobTemp = metadataJob.getLatestDictJob(JobState.PENDING, JobState.RUNNING);
        JobState state = jobTemp.getState();

        Assert.assertEquals(true, (state == JobState.PENDING) || (state == JobState.RUNNING));
        metadataJob.deleteJob(job.getId());
    }


    public void updateJob() {
        metadataJob.addJob(job);
        Job jobTemp = metadataJob.getJob(job.getId());
        jobTemp.setLatestTask(task0.getId());
        jobTemp.setState(JobState.RUNNING);

        metadataJob.updateJob(jobTemp);
        Assert.assertEquals(task0.getId(),
                metadataJob.getJob(job.getId()).getLatestTask());
        Assert.assertEquals(JobState.RUNNING, metadataJob.getJob(job.getId()).getState());
        metadataJob.deleteJob(job.getId());
    }


    public void updateTask() {

        metadataJob.addJob(job);
        metadataJob.addTask(task0);

        Task task = metadataJob.getTask(task0.getId());
        task.setAppId("appid_test_20200227");
        task.setState(TaskState.RUNNING);
        metadataJob.updateTask(task);

        Assert.assertEquals("appid_test_20200227",
                metadataJob.getTask(task0.getId()).getAppId());
        Assert.assertEquals(TaskState.RUNNING, metadataJob.getTask(task0.getId()).getState());

        metadataJob.deleteJob(job.getId());
    }


    public void updateTaskAppId() {

        metadataJob.addJob(job);
        metadataJob.addTask(task0);

        metadataJob.updateTaskAppId(task0.getId(), "appid_test_20200227_1");
        Assert.assertEquals("appid_test_20200227_1",
                metadataJob.getTask(task0.getId()).getAppId());

        metadataJob.deleteJob(job.getId());
    }


    public void updateTaskStatus() {

        metadataJob.addJob(job);
        metadataJob.addTask(task0);

        metadataJob.updateTaskStatus(task0.getId(), TaskState.FAILED);
        Assert.assertEquals(TaskState.FAILED, metadataJob.getTask(task0.getId()).getState());

        metadataJob.deleteJob(job.getId());
    }


    public void completeTask() {

        metadataJob.addJob(job);
        metadataJob.addTask(task0);

        metadataJob.completeTask(
                task0.getId(),
                TaskState.SUCCESS,
                DateUtil.toLocalDateTime("2020-02-05 10:12:05"),
                "/user/hive/tagbase/dict/forward/20200227");

        Assert.assertEquals(TaskState.SUCCESS, metadataJob.getTask(task0.getId()).getState());
        Assert.assertEquals("/user/hive/tagbase/dict/forward/20200227",
                metadataJob.getTask(task0.getId()).getOutput());

        metadataJob.deleteJob(job.getId());
    }


    public void listNotCompletedJob() {

        metadataJob.addJob(job);

        Assert.assertEquals(1, metadataJob.listNotCompletedJob(job.getDbName(), job.getTableName(),
                DateUtil.toLocalDateTime("2020-02-26 10:10:05"),
                now()).size());

        metadataJob.deleteJob(job.getId());


    }


    public void getRunningDictJob() {

        metadataJob.addJob(job);

        Job jobTemp = metadataJob.getJob(job.getId());
        jobTemp.setState(JobState.RUNNING);

        metadataJob.updateJob(jobTemp);

        Job job = metadataJob.getRunningDictJob();
        Assert.assertEquals(JobState.RUNNING, job.getState());
        Assert.assertEquals(JobType.DICTIONARY, job.getType());

        metadataJob.deleteJob(job.getId());
    }


    public void getTask() {

        metadataJob.addJob(job);
        metadataJob.addTask(task1);

        Task task = metadataJob.getTask(job.getId(), (byte) 1);
        Assert.assertEquals(task1.getId(), task.getId());
        Assert.assertEquals((byte) 1, task.getStep());

        metadataJob.deleteJob(job.getId());
    }


    public void updateJobStartTime() {

        metadataJob.addJob(job);

        LocalDateTime startTime = DateUtil.toLocalDateTime("2020-02-27 11:12:05");
        metadataJob.updateJobStartTime(job.getId(), startTime);
        Assert.assertEquals(startTime, metadataJob.getJob(job.getId()).getStartTime());
        metadataJob.deleteJob(job.getId());

    }


    public void updateTaskStartTime() {

        metadataJob.addJob(job);
        metadataJob.addTask(task0);

        LocalDateTime startTime = DateUtil.toLocalDateTime("2020-02-27 11:15:05");
        metadataJob.updateTaskStartTime(task0.getId(), startTime);
        Assert.assertEquals(startTime, metadataJob.getTask(task0.getId()).getStartTime());

        metadataJob.deleteJob(job.getId());

    }


    public void updateTaskOutput() {

        metadataJob.addJob(job);
        metadataJob.addTask(task0);

        String out = "/user/hive/tagbase/dict/inverted/20200227_new";
        metadataJob.updateTaskOutput(task0.getId(), out);
        Assert.assertEquals(out, metadataJob.getTask(task0.getId()).getOutput());

        metadataJob.deleteJob(job.getId());
    }


    public void updateTaskEndTime() {

        metadataJob.addJob(job);
        metadataJob.addTask(task0);

        LocalDateTime endTime = DateUtil.toLocalDateTime("2020-02-27 12:15:05");
        metadataJob.updateTaskEndTime(task0.getId(), endTime);
        Assert.assertEquals(endTime, metadataJob.getTask(task0.getId()).getEndTime());

        metadataJob.deleteJob(job.getId());
    }


    public void updateJobEndTime() {

        metadataJob.addJob(job);

        LocalDateTime endTime = DateUtil.toLocalDateTime("2020-02-27 13:12:05");
        metadataJob.updateJobEndTime(job.getId(), endTime);
        Assert.assertEquals(endTime, metadataJob.getJob(job.getId()).getEndTime());

        metadataJob.deleteJob(job.getId());

    }


    public void updateJobStatus() {

        metadataJob.addJob(job);

        metadataJob.updateJobStatus(job.getId(), JobState.FAILED);
        Assert.assertEquals(JobState.FAILED, metadataJob.getJob(job.getId()).getState());

        metadataJob.deleteJob(job.getId());

    }


    public void completeJob() {

        metadataJob.addJob(job);

        metadataJob.completeJob(job.getId(),
                JobState.SUCCESS,
                DateUtil.toLocalDateTime("2020-02-27 11:12:05"));
        Assert.assertEquals(JobState.SUCCESS, metadataJob.getJob(job.getId()).getState());
        Assert.assertEquals("2020-02-27T11:12:05",
                metadataJob.getJob(job.getId()).getEndTime().toString());

        metadataJob.deleteJob(job.getId());
    }


    public void listSuccessDictJobs() {

        job.setState(JobState.SUCCESS);
        metadataJob.addJob(job);

        LocalDateTime dataLowerTime = DateUtil.toLocalDateTime("2020-02-25 10:12:05");
        LocalDateTime dataUpperTime = DateUtil.toLocalDateTime("2020-02-28 10:12:05");
        List<Job> jobs = metadataJob.listSuccessDictJobs(dataLowerTime, dataUpperTime);
        Assert.assertEquals(JobState.SUCCESS, jobs.get(0).getState());

        metadataJob.deleteJob(job.getId());
    }


    public void deleteJob() {

        metadataJob.addJob(job);

        Assert.assertEquals("DictBuild20200227",
                metadataJob.getJob("DictBuild20200227").getId());

        metadataJob.deleteJob("DictBuild20200227");

    }


}