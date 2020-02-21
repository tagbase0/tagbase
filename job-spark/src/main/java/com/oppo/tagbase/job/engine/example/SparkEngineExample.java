package com.oppo.tagbase.job.engine.example;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.oppo.tagbase.common.guice.*;
import com.oppo.tagbase.job.TaskEngine;
import com.oppo.tagbase.job.engine.SparkTaskEngineModule;
import com.oppo.tagbase.job.obj.*;
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
                new SparkTaskEngineModule()
        );

        TaskEngine executable = ij.getInstance(TaskEngine.class);

        HiveDictTable hiveDictTable = new HiveDictTable("default","imeiTable","imei","id","daynum",0);
        SliceColumn sliceColumn = new SliceColumn("daynum","20200220");
        List<String> dimColumns = new ArrayList<String>(){{add("app");add("event");add("version");}};
        HiveSrcTable hiveSrcTable = new HiveSrcTable("default","eventTable",dimColumns,sliceColumn,"imei");
        HiveMeta hiveMeta = new HiveMeta(hiveDictTable,hiveSrcTable,"D:\\workStation\\sparkTaskHfile\\city_20200211_task");

        try {
            String appid= executable.submitJob(hiveMeta, JobType.BITMAP_BUILDING);
            System.out.println("appid ： " + appid );
//            JobMessage jobMessage = executable.getJobStatus("appid", JobType.BITMAP_BUILDING);
//            System.out.println("status ： " + jobMessage.parseJobStatus() );
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
