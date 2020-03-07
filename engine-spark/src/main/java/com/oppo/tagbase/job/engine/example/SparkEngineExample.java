package com.oppo.tagbase.job.engine.example;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.oppo.tagbase.common.guice.*;
import com.oppo.tagbase.job.engine.SparkConfigConstant;
import com.oppo.tagbase.job.engine.SparkTaskEngineModule;
import com.oppo.tagbase.job.engine.example.testobj.TestDictHiveInputConfigModule;
import com.oppo.tagbase.job.engine.example.testobj.TestJobConfigModule;
import com.oppo.tagbase.jobv2.DictHiveInputConfig;
import com.oppo.tagbase.jobv2.JobConfig;
import com.oppo.tagbase.jobv2.spi.DataTaskContext;
import com.oppo.tagbase.jobv2.spi.DictTaskContext;
import com.oppo.tagbase.jobv2.spi.TaskEngine;
import com.oppo.tagbase.meta.obj.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by liangjingya on 2020/2/20.
 */
public class SparkEngineExample {

    public static void main(String[] args) {

        Injector ij = ExampleGuiceInjectors.makeInjector(
                new ValidatorModule(),
                new PropsModule(ImmutableList.of("tagbase.properties")),
                new JacksonModule(),
                new SparkTaskEngineModule(),
                new TestJobConfigModule(),
                new TestDictHiveInputConfigModule()
        );

        TaskEngine executable = ij.getInstance(TaskEngine.class);
        JobConfig jobConfig = ij.getInstance(JobConfig.class);
        DictHiveInputConfig hiveConfig = ij.getInstance(DictHiveInputConfig.class);


        buildData(executable, jobConfig);

//       buildDict(executable, jobConfig, hiveConfig);

    }

    public static void  buildData(TaskEngine executable, JobConfig jobConfig) {

        Table table = new Table();
        table.setSrcDb("default");
        table.setSrcTable("age");
        List<Column> columns = new ArrayList<>();
        Column a=new Column();a.setSrcName("version");a.setIndex(3);a.setType(ColumnType.DIM_COLUMN);
        columns.add(a);
        Column b=new Column();b.setSrcName("app");b.setIndex(1);b.setType(ColumnType.DIM_COLUMN);
        columns.add(b);
        Column c=new Column();c.setSrcName("event");c.setIndex(2);c.setType(ColumnType.DIM_COLUMN);
        columns.add(c);
        Column d=new Column();d.setSrcName("imei");d.setIndex(-1);d.setType(ColumnType.BITMAP_COLUMN);
        columns.add(d);
        Column e=new Column();e.setSrcName("daynum");e.setIndex(-1);e.setType(ColumnType.SLICE_COLUMN);
        e.setSrcPartColDateFormat(ColDateFormat.HIVE_DATE);e.setSrcDataType(ResourceColType.NUMBER);
        columns.add(e);
        table.setColumns(columns);

        List<Props> propsList = new ArrayList<>();
        Props prop = new Props();
        prop.setKey(SparkConfigConstant.EXECUTOR_MEMORY);prop.setValue("666m");
        Props prop2 = new Props();
        prop2.setKey(SparkConfigConstant.EXECUTOR_INSTANCES);prop2.setValue("2");
        Props prop3 = new Props();
        prop3.setKey(SparkConfigConstant.DEFAULT_PARALLELISM);prop3.setValue("2");
        propsList.add(prop);propsList.add(prop2);propsList.add(prop3);
        table.setProps(propsList);

        DataTaskContext context = new DataTaskContext("jobId", "taskId",
                table, jobConfig,
                LocalDateTime.of(2020,3,2,2,0,0),
                LocalDateTime.of(2020,3,3,2,0,0));

        String appId = executable.buildData(context);
        System.out.println("buildData appId: " + appId);
    }


    public static void  buildDict(TaskEngine executable, JobConfig jobConfig, DictHiveInputConfig hiveConfig) {

        Table table = new Table();
        List<Props> propsList = new ArrayList<>();
        Props prop = new Props();
        prop.setKey(SparkConfigConstant.EXECUTOR_MEMORY);prop.setValue("666m");
        Props prop2 = new Props();
        prop2.setKey(SparkConfigConstant.EXECUTOR_INSTANCES);prop2.setValue("2");
        Props prop3 = new Props();
        prop3.setKey(SparkConfigConstant.DEFAULT_PARALLELISM);prop3.setValue("2");
        propsList.add(prop);propsList.add(prop2);propsList.add(prop3);
        table.setProps(propsList);

        DictTaskContext context = new DictTaskContext(table, "jobId", "taskId",
                hiveConfig, jobConfig,0L,
                LocalDateTime.of(2020,3,2,2,0,0),
                LocalDateTime.of(2020,3,3,2,0,0));

        String appId = executable.buildDict(context);
        System.out.println("buildDict appId: " + appId);
    }
}
