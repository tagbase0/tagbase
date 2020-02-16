package com.oppo.tagbase.job;

import com.oppo.tagbase.job.util.IdGenerator;
import com.oppo.tagbase.meta.obj.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;

/**
 * Created by daikai on 2020/2/16.
 */
public class DictJob extends AbstractJob {

    protected static String DICT_JOB_ID ;

     Logger log = LoggerFactory.getLogger(DictJob.class);

    @Override
    public boolean succeed(String jobId) {
        if(State.SUCCEEDED == this.getState()){
            return true;
        }
        return false;
    }

    @Override
    public boolean checkBuildState(){
        //TODO 2020/2/16 此处若imei没有更新，可跳过字典构建任务


        return true;
    }

    @Override
    public String build(String dbName, String tableName) {

        DictJob dictJob = new DictJob();
        dictJob.setStartTime(System.currentTimeMillis());

        // 加上date参数，限制字典构建一天只进行一次;
        // 若不带date参数，字典一天可构建多次
        DICT_JOB_ID = new IdGenerator().nextQueryId("DictBuildJob", "yyyyMMdd");
        dictJob.setId(DICT_JOB_ID);

        String today = new SimpleDateFormat("yyyyMMdd").format(System.currentTimeMillis());
        dictJob.setJobName("DictBuildJob_" + today);
        dictJob.setType(JobType.DICT);
        dictJob.setState(State.PENDING);

        log.info("{} is pending", DICT_JOB_ID);

        if(checkBuildState()){
            buildDict(dictJob);
            dictJob.setEndTime(System.currentTimeMillis());
            dictJob.setState(State.SUCCEEDED);
            log.info("{} is finished", DICT_JOB_ID);
        }else {
            log.info("Skip to build dictionary !");
        }


        return DICT_JOB_ID;
    }

    public Slice buildDict(DictJob dictJob){
        dictJob.setState(State.RUNNING);
        dictJob.setRuntime(System.currentTimeMillis());
        log.info("{} is running", DICT_JOB_ID);
        //TODO 2020/2/16  build dictionary



        return null;
    }


}
