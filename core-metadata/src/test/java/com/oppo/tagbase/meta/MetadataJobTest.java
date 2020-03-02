package com.oppo.tagbase.meta;

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.oppo.tagbase.common.guice.PropsModule;
import com.oppo.tagbase.common.guice.ValidatorModule;
import com.oppo.tagbase.meta.connector.MetaStoreConnectorConfig;
import com.oppo.tagbase.meta.obj.*;
import org.junit.Assert;
import org.junit.Before;


import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static java.time.LocalDateTime.now;

/**
 * Created by daikai on 2020/2/27.
 */
public class MetadataJobTest {

    MetadataJob metadataJob;

    @Before
    public void setup() {
        Injector injector = Guice.createInjector(
                new ValidatorModule(),
                new PropsModule(Lists.newArrayList("tagbase.properties")),
                new MetadataModule()
        );

        MetaStoreConnectorConfig c = injector.getInstance(MetaStoreConnectorConfig.class);

        metadataJob = injector.getInstance(MetadataJob.class);
    }


    public void addJob() {
        Job job = new Job();
        job.setId("DictBuild20200227");
        job.setName("DictBuild20200227_1");
        job.setDbName("hive");
        job.setTableName("gobal_imei");
        job.setStartTime(LocalDateTime.parse("2020-02-27 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        job.setState(JobState.PENDING);
        job.setLatestTask("");
        job.setDataLowerTime(LocalDateTime.parse("2020-02-27 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        job.setDataUpperTime(LocalDateTime.parse("2020-02-27 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        job.setType(JobType.DICTIONARY);

        metadataJob.addJob(job);

        Assert.assertEquals(job, metadataJob.getJob(job.getId()));
    }


    public void getJob() {
        Assert.assertEquals("DictBuild20200227", metadataJob.getJob("DictBuild20200227").getId());
    }


    public void addTask() {
        Task task = new Task();
        task.setId("TaskDictInvertedBuild20200227");
        task.setName("TaskDictInvertedBuild20200227_1");
        task.setState(TaskState.PENDING);
        task.setOutput("/user/hive/tagbase/dict/inverted/20200227");
        task.setStep((byte) 0);
        task.setJobId("DictBuild20200227");
        task.setStartTime(LocalDateTime.parse("2020-02-27 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

        Task task1 = new Task();
        task1.setId("TaskDictForwardBuild20200227");
        task1.setName("TaskDictForwardBuild20200227_1");
        task1.setState(TaskState.PENDING);
        task1.setOutput("/user/hive/tagbase/dict/forward/20200227");
        task1.setStep((byte) 1);
        task1.setJobId("DictBuild20200227");
        task1.setStartTime(LocalDateTime.parse("2020-02-27 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

        metadataJob.addTask(task1);
        metadataJob.addTask(task);

        Assert.assertEquals(task, metadataJob.getTask(task.getId()));
        Assert.assertEquals(task1, metadataJob.getTask(task1.getId()));
    }


    public void updateJob() {
        Job job = metadataJob.getJob("DictBuild20200227");
        job.setLatestTask("TaskDictInvertedBuild20200227");
        job.setState(JobState.RUNNING);

        metadataJob.updateJob(job);
        Assert.assertEquals("TaskDictInvertedBuild20200227",
                metadataJob.getJob("DictBuild20200227").getLatestTask());
        Assert.assertEquals(JobState.RUNNING, metadataJob.getJob("DictBuild20200227").getState());
    }


    public void updateTask() {
        Task task = metadataJob.getTask("TaskDictInvertedBuild20200227");
        task.setAppId("appid_test_20200227");
        task.setState(TaskState.RUNNING);
        metadataJob.updateTask(task);

        Assert.assertEquals("appid_test_20200227",
                metadataJob.getTask("TaskDictInvertedBuild20200227").getAppId());

        Assert.assertEquals(TaskState.RUNNING, metadataJob.getTask("TaskDictInvertedBuild20200227").getState());
    }


    public void updateTaskAppId() {
        metadataJob.updateTaskAppId("TaskDictInvertedBuild20200227", "appid_test_20200227_1");
        Assert.assertEquals("appid_test_20200227_1",
                metadataJob.getTask("TaskDictInvertedBuild20200227").getAppId());
    }


    public void updateTaskStatus() {
        metadataJob.updateTaskStatus("TaskDictInvertedBuild20200227", TaskState.FAILED);
        Assert.assertEquals(TaskState.FAILED, metadataJob.getTask("TaskDictInvertedBuild20200227").getState());
    }



    public void completeTask() {
        metadataJob.completeTask("TaskDictInvertedBuild20200227",
                TaskState.SUCCESS,
                LocalDateTime.parse("2020-02-05 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                "/user/hive/tagbase/dict/forward/20200227");

        Assert.assertEquals(TaskState.SUCCESS, metadataJob.getTask("TaskDictInvertedBuild20200227").getState());
        Assert.assertEquals("/user/hive/tagbase/dict/forward/20200227",
                metadataJob.getTask("TaskDictInvertedBuild20200227").getOutput());

    }


    public void listNotCompletedJob() {
        Assert.assertEquals(1, metadataJob.listNotCompletedJob("hive", "gobal_imei",
                LocalDateTime.parse("2020-02-27 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                now()).size());
    }



    public void getRunningDictJob() {

        Job job = metadataJob.getRunningDictJob();
        Assert.assertEquals(JobState.RUNNING, job.getState());
        Assert.assertEquals(JobType.DICTIONARY, job.getType());
    }


    public void getTask() {
        Task task = metadataJob.getTask("DictBuild20200227", (byte)1);
        Assert.assertEquals("TaskDictForwardBuild20200227", task.getId());
        Assert.assertEquals((byte)1, task.getStep());
    }


    public void listPendingJobs() {
        List <Job> jobs = metadataJob.listPendingJobs();
        Assert.assertEquals(JobState.PENDING, jobs.get(0).getState());
    }


    public void updateJobStartTime() {

        LocalDateTime startTime = LocalDateTime.parse("2020-02-27 11:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        metadataJob.updateJobStartTime("DictBuild20200227", startTime);
        Assert.assertEquals(startTime, metadataJob.getJob("DictBuild20200227").getStartTime());

    }



    
    public void updateTaskStartTime() {

        LocalDateTime startTime = LocalDateTime.parse("2020-02-27 11:15:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        metadataJob.updateTaskStartTime("TaskDictInvertedBuild20200227", startTime);
        Assert.assertEquals(startTime, metadataJob.getTask("TaskDictInvertedBuild20200227").getStartTime());
    }

    
    public void updateTaskOutput() {
        String out = "/user/hive/tagbase/dict/inverted/20200227_new";
        metadataJob.updateTaskOutput("TaskDictInvertedBuild20200227", out);
        Assert.assertEquals(out, metadataJob.getTask("TaskDictInvertedBuild20200227").getOutput());
    }


    public void updateTaskEndTime() {

        LocalDateTime endTime = LocalDateTime.parse("2020-02-27 12:15:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        metadataJob.updateTaskEndTime("TaskDictInvertedBuild20200227", endTime);
        Assert.assertEquals(endTime, metadataJob.getTask("TaskDictInvertedBuild20200227").getEndTime());
    }



    public void updateJobEndTime() {

        LocalDateTime endTime = LocalDateTime.parse("2020-02-27 13:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        metadataJob.updateJobEndTime("DictBuild20200227", endTime);
        Assert.assertEquals(endTime, metadataJob.getJob("DictBuild20200227").getEndTime());

    }


    public void updateJobStatus() {
        Assert.assertEquals(JobState.RUNNING, metadataJob.getJob("DictBuild20200227").getState());
    }


    public void completeJob() {
        metadataJob.completeJob("DictBuild20200227",
                JobState.SUCCESS,
                LocalDateTime.parse("2020-02-27 11:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        Assert.assertEquals(JobState.SUCCESS, metadataJob.getJob("DictBuild20200227").getState());
        Assert.assertEquals("2020-02-27T11:12:05",
                metadataJob.getJob("DictBuild20200227").getEndTime().toString());
    }


    public void listSuccessDictJobs() {

        LocalDateTime dataLowerTime = LocalDateTime.parse("2020-02-26 13:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        LocalDateTime dataUpperTime = LocalDateTime.parse("2020-02-28 13:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        List<Job> jobs = metadataJob.listSuccessDictJobs(dataLowerTime, dataUpperTime);
        Assert.assertEquals(JobState.SUCCESS, jobs.get(0).getState());

    }

    public void deleteJob() {
        Assert.assertEquals("DictBuild20200227",
                metadataJob.getJob("DictBuild20200227").getId());

        metadataJob.deleteJob("DictBuild20200227");

    }


}