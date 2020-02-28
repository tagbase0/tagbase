package com.oppo.tagbase.job.engine.example;

//import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.oppo.tagbase.common.guice.*;
import com.oppo.tagbase.job.engine.SparkTaskEngineModule;
import com.oppo.tagbase.job.engine.TaskEngine;
import com.oppo.tagbase.job.engine.obj.HiveMeta;
import com.oppo.tagbase.job.engine.obj.HiveSrcTable;
import com.oppo.tagbase.job.engine.obj.JobType;
import com.oppo.tagbase.job.engine.obj.SliceColumn;

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

        SliceColumn sliceColumn = new SliceColumn("daynum","20200220","20200221");
        List<String> dimColumns = new ArrayList<String>(){{add("app");add("event");add("version");}};
        HiveSrcTable hiveSrcTable = new HiveSrcTable("default","eventTable",dimColumns,sliceColumn,"imei");
        HiveMeta hiveMeta = new HiveMeta("D:\\workStation\\tagbase\\invertedDict\\",10, hiveSrcTable,"D:\\workStation\\tagbase\\jobidxxxx\\hfile\\","D:\\workStation\\tagbase\\jobidxxxx\\rowCount");

        try {
//            ObjectMapper objectMapper=new ObjectMapper();
//            String appArgs = objectMapper.writeValueAsString(hiveMeta);
//            System.out.println(appArgs);

            String appid= executable.submitTask(hiveMeta, JobType.DATA);
            System.out.println("appid ： " + appid );

//            JobMessage jobMessage = executable.getJobStatus("appid", JobType.BITMAP_BUILDING);
//            System.out.println("status ： " + jobMessage.parseJobStatus() );
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}