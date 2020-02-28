package com.oppo.tagbase.meta;

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.oppo.tagbase.common.guice.PropsModule;
import com.oppo.tagbase.common.guice.ValidatorModule;
import com.oppo.tagbase.meta.connector.MetaStoreConnectorConfig;
import com.oppo.tagbase.meta.obj.*;
import org.junit.Before;


import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

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
        job.setId("DictForwardBuild20200226");
        job.setName("DictForwardBuild20200226_1");
        job.setDbName("hive");
        job.setTableName("gobal_imei");
        job.setStartTime(LocalDateTime.parse("2020-02-04 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        job.setState(JobState.PENDING);
        job.setLatestTask("");
        job.setDataLowerTime(LocalDateTime.parse("2020-02-04 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        job.setDataUpperTime(LocalDateTime.parse("2020-02-05 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        job.setType(JobType.DICTIONARY);

        metadataJob.addJob(job);
    }

    
    public void getJob() {
        System.out.println(metadataJob.getJob("DictForwardBuild20200226"));
    }


    
    public void addTask() {
        Task task = new Task();
        task.setId("TaskDictInvertedBuild20200205");
        task.setName("TaskDictInvertedBuild20200205_1");
        task.setState(TaskState.PENDING);
        task.setOutput("/user/hive/tagbase/dict/forward/20200205");
        task.setStep((byte)0);
        task.setJobId("DictForwardBuild20200205");
        task.setStartTime(LocalDateTime.parse("2020-02-05 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

        Task task1 = new Task();
        task1.setId("TaskDictForwardBuild20200205");
        task1.setName("TaskDictForwardBuild20200205_1");
        task1.setState(TaskState.PENDING);
        task1.setOutput("/user/hive/tagbase/dict/forward/20200205");
        task1.setStep((byte)1);
        task1.setJobId("DictForwardBuild20200205");
        task1.setStartTime(LocalDateTime.parse("2020-02-05 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));

        metadataJob.addTask(task1);
        metadataJob.addTask(task);
    }

    
    public void completeTask() {
        metadataJob.completeTask("TaskDictForwardBuild20200205",
                TaskState.SUCCESS,
                LocalDateTime.parse("2020-02-05 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                "/user/hive/tagbase/dict/forward/20200205");
    }

    
    public void completeJOb() {
        metadataJob.completeJOb("DictForwardBuild20200226",
                JobState.SUCCESS,
                LocalDateTime.parse("2020-02-05 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        );
    }

    
    public void deleteJOb() {
        metadataJob.deleteJOb("DictForwardBuild20200226");
    }

    
    public void updateJob() {
        Job job = metadataJob.getJob("DictForwardBuild20200226");
        job.setLatestTask("DictForwardBuild20200226_2");
        job.setState(JobState.RUNNING);

        metadataJob.updateJob(job);
    }

    
    public void updateTask() {
        Task task = metadataJob.getTask("TaskDictInvertedBuild20200205");
        task.setAppId("appid_test_20200205");
        task.setState(TaskState.RUNNING);
        metadataJob.updateTask(task);
    }
}