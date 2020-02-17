package com.oppo.tagbase.job;

import com.oppo.tagbase.job.util.IdGenerator;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobState;
import com.oppo.tagbase.meta.obj.JobType;
import com.oppo.tagbase.meta.obj.Slice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.text.SimpleDateFormat;


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
    public String build(String dbName, String tableName, String jobType) {
        if ("dictionary".equalsIgnoreCase(jobType)) {
            return build(dbName, tableName);
        } else {
            return null;
        }
    }

    @Override
    public void iniJob(Job job) {

        new MetadataJob().createJob(job);

    }

    public String build(String dbName, String tableName) {

        Job dictJob = new Job();

        DICT_JOB_ID = new IdGenerator().nextQueryId("DictBuildJob", "yyyyMMdd");
        String today = new SimpleDateFormat("yyyyMMdd").format(System.currentTimeMillis());

        dictJob.setId(DICT_JOB_ID);
        dictJob.setName("DictBuildJob_" + today);
        dictJob.setDbName(dbName);
        dictJob.setTableName(tableName);
        dictJob.setSliceName("");
        dictJob.setStartTime(new Date(System.currentTimeMillis()));
        dictJob.setLatestTask("");
        dictJob.setState(JobState.PENDING);
        dictJob.setType(JobType.DICTIONARY);

        iniJob(dictJob);

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

    public Slice buildDict(Job dictJob) {
        dictJob.setState(JobState.RUNNING);
        log.info("{} is running", DICT_JOB_ID);
        //TODO 2020/2/16  build dictionary


        return null;
    }


}