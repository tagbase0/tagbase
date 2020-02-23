package com.oppo.tagbase.job;

import com.google.inject.Injector;
import com.oppo.tagbase.job.obj.HiveMeta;
import com.oppo.tagbase.job.obj.TaskMessage;
import com.oppo.tagbase.job.util.TableHelper;
import com.oppo.tagbase.job.util.TaskHelper;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.*;
import com.oppo.tagbase.storage.core.exception.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.List;
import java.util.concurrent.Callable;

import com.oppo.tagbase.storage.core.connector.StorageConnector;

/**
 * Created by daikai on 2020/2/16.
 */
public class BitMapBuildJob extends Task implements Callable<Slice> {

    Logger log = LoggerFactory.getLogger(BitMapBuildJob.class);
    private Injector injector;

    private String jobId;

    public BitMapBuildJob(String jobId) {
        this.jobId = jobId;
    }

    @Override
    public Slice call() {

        Job jobRunning = new MetadataJob().getJob(jobId);
        jobRunning.setState(JobState.RUNNING);
        List<Task> tasks = jobRunning.getTasks();
        int stepNum = tasks.size();
        HiveMeta hiveMeta = null;

        //TODO 2020/2/16  分步骤执行
        // if all the tasks of the job finished, jump out of the loop
        for (int i = 0; jobRunning.getState() != JobState.SUCCESS; i++) {
            switch (i) {
                case 0:
                    // task is started only if the previous task has been executed successfully
                    if (new TaskHelper().preTaskFinish(tasks, i)) {
                        // BitmapBuildingTask();
                        // 1. update Metadata, construct the HiveMeta object
                        Task task = tasks.get(i);
                        task.setState(TaskState.RUNNING);
                        jobRunning.setLatestTask(task.getId());

                        String toPath = task.getOutput();

                        hiveMeta = new TableHelper().generalHiveMeta(task, jobRunning);
                        log.debug("BitmapBuildingTask {} start.", task.getId());

                        // 2. start the engine task
                        TaskEngine taskEngine = injector.getInstance(TaskEngine.class);
                        String appId = null;
                        TaskMessage taskMessage = null;
                        TaskState state = null;

                        try {
                            appId = taskEngine.submitTask(hiveMeta, JobType.DICTIONARY);
                            taskMessage = taskEngine.getTaskStatus(appId, JobType.DICTIONARY);

                        } catch (Exception e) {
                            log.error("{}, Error to run TaskEngine!", task.getId());
                        }

                        task.setAppId(appId);
                        //TODO convert finalStatus to TaskState

                        //3. update Metadata
                        new MetadataJob().completeTask(task.getId(), state,
                                new Date(System.currentTimeMillis()), toPath);

                        log.debug("BitmapBuildingTask {} finished.", task.getId());
                    }
                    break;
                case 1:
                    if (new TaskHelper().preTaskFinish(tasks, i)) {
                        // bulkload() && addSlice();
                        // 1. update Metadata, construct the parameters of the task
                        Task task = tasks.get(i);
                        task.setState(TaskState.RUNNING);
                        jobRunning.setLatestTask(task.getId());
                        String dbName = jobRunning.getDbName();
                        String tableName = jobRunning.getTableName();
                        String srcPath = tasks.get(i - 1).getOutput();
                        TaskState state = TaskState.SUCCESS;

                        log.debug("BulkLoadTask {} start.", task.getId());

                        //2. do task
                        StorageConnector storageConnector = null;
                        String sink;
                        if(new TableHelper().firstBuildTag(dbName, tableName)){
                            sink = new TableHelper().sinkName(dbName, tableName);
                        }else{
                            sink = new Metadata().getSlices(dbName, tableName).get(0).getSink();
                        }

                        try {
                            storageConnector.createBatchRecords("", sink, srcPath);
                        } catch (StorageException e) {
                            log.error("{}, Error bulk load to HBase!", task.getId());
                            state = TaskState.FAILED;
                        }

                        Slice slice = new TaskHelper().constructSlice(hiveMeta, sink);

                        //3. update Metadata
                        new MetadataJob().completeTask(task.getId(), state,
                                new Date(System.currentTimeMillis()), task.getOutput());
                        log.debug("BulkLoadTask {} finished.", task.getId());

                        new Metadata().addSlice(slice);

                        if(TaskState.SUCCESS == state){
                            jobRunning.setState(JobState.SUCCESS);
                        }
                    }
                    break;

                default:

            }
            i = i % stepNum;
        }

        // job are removed from the pending queue only after success
        if (JobState.SUCCESS == jobRunning.getState()) {
            AbstractJob.PENDING_JOBS_QUEUE.remove(new MetadataJob().getJob(jobId));
        }

        return null;
    }


}
