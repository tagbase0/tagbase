package com.oppo.tagbase.job;

import com.google.inject.Injector;
import com.oppo.tagbase.job.engine.obj.HiveMeta;
import com.oppo.tagbase.job.engine.obj.TaskMessage;
import com.oppo.tagbase.job.util.TableHelper;
import com.oppo.tagbase.job.util.TaskHelper;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobState;
import com.oppo.tagbase.meta.obj.JobType;
import com.oppo.tagbase.meta.obj.Slice;
import com.oppo.tagbase.meta.obj.Task;
import com.oppo.tagbase.meta.obj.TaskState;
import com.oppo.tagbase.storage.core.connector.StorageConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.Callable;

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

        // if all the tasks of the job finished, jump out of the loop
        for (int i = 0; jobRunning.getState() != JobState.SUCCESS; i++) {
            switch (i) {
                case 0:
                    // task is started only if the previous task has been executed successfully
                    if (new TaskHelper().preTaskFinish(tasks, i)) {
                        // BitmapBuildingTask();

                        Task task = tasks.get(i);
                        hiveMeta = doBitmapBuildingTask(task, jobRunning);

                        log.debug("BitmapBuildingTask {} finished.", task.getId());
                    }
                    break;
                case 1:
                    if (new TaskHelper().preTaskFinish(tasks, i)) {
                        // bulkload() && addSlice();

                        Task task = tasks.get(i);
                        doLoad(task, jobRunning, hiveMeta);

                        if (TaskState.SUCCESS == task.getState()) {
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

    private void doLoad(Task task, Job jobRunning, HiveMeta hiveMeta) {

        // 1. update Metadata, construct the parameters of the task
        task.setState(TaskState.RUNNING);
        jobRunning.setLatestTask(task.getId());
        String dbName = jobRunning.getDbName();
        String tableName = jobRunning.getTableName();
        TaskState state = TaskState.SUCCESS;
        String srcPath = hiveMeta.getOutput();

        log.debug("BulkLoadTask {} start.", task.getId());

        //2. do task
        StorageConnector storageConnector = null;
        String sink;
        if (new TableHelper().firstBuildTag(dbName, tableName)) {
            sink = new TableHelper().sinkName(dbName, tableName);
        } else {
            sink = new Metadata().getSlices(dbName, tableName).get(0).getSink();
        }

//        try {
//            storageConnector.createBatchRecords("", sink, srcPath);
//        } catch (StorageException e) {
//            log.error("{}, Error bulk load to HBase!", task.getId());
//            state = TaskState.FAILED;
//        }

        Slice slice = new TaskHelper().constructSlice(hiveMeta, sink);

        //3. update Metadata
        new MetadataJob().completeTask(task.getId(), state,
                LocalDateTime.now(), task.getOutput());
        log.debug("BulkLoadTask {} finished.", task.getId());

        new Metadata().addSlice(slice);
    }

    private HiveMeta doBitmapBuildingTask(Task task, Job jobRunning) {

        // 1. update Metadata, construct the HiveMeta object
        task.setState(TaskState.RUNNING);
        jobRunning.setLatestTask(task.getId());

        String toPath = task.getOutput();

        HiveMeta hiveMeta = new TableHelper().generalHiveMeta(task, jobRunning);
        log.debug("BitmapBuildingTask {} start.", task.getId());

        // 2. start the engine task
        TaskEngine taskEngine = injector.getInstance(TaskEngine.class);
        String appId = null;
        TaskMessage taskMessage = null;
        TaskState state;

        try {
            appId = taskEngine.submitTask(hiveMeta, JobType.DICTIONARY);
            taskMessage = taskEngine.getTaskStatus(appId, JobType.DICTIONARY);

        } catch (Exception e) {
            log.error("{}, Error to run TaskEngine!", task.getId());
        }

        task.setAppId(appId);
        state = taskMessage.parseJobStatus();

        //3. update Metadata
        new MetadataJob().completeTask(task.getId(), state,
                LocalDateTime.now(), toPath);

        return hiveMeta;
    }


}
