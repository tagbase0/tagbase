package com.oppo.tagbase.job;

import com.oppo.tagbase.job.obj.HiveMeta;
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

    private String taskId;
    private String appId;
    private long startTime;
    private long endTime;
    private TaskState taskState;
    int step;
    private String jobId;

    public BitMapBuildJob(String jobId) {
        this.jobId = jobId;
    }

    public BitMapBuildJob(String taskId, String appId, long startTime, long endTime,
                          TaskState taskState, int step, String jobId) {
        this.taskId = taskId;
        this.appId = appId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.taskState = taskState;
        this.step = step;
        this.jobId = jobId;
    }


    @Override
    public Slice call() {

        Job jobRunning = new MetadataJob().getJob(jobId);
        jobRunning.setState(JobState.RUNNING);
        List<Task> tasks = jobRunning.getTasks();
        int stepNum = tasks.size();

        //TODO 2020/2/16  分步骤执行
        // 当该 job 下所有子任务都执行成功，则循环结束
        for (int i = 0; jobRunning.getState() != JobState.SUCCESS; i++) {
            switch (i) {
                case 0:
                    // 仅仅当前任务的前置任务都正常执行成功，才开启这个任务
                    if (new TaskHelper().preTaskFinish(tasks, i)) {
                        // BitmapBuildingTask();
                        // 1. task输入参数获取
                        // 参数 反向字典hdfs路径, 标签 dbName 和 tableName, 以及生成的 Hfile 的存放路径
                        Task task = tasks.get(i);
                        task.setState(TaskState.RUNNING);
                        jobRunning.setLatestTask(task.getId());

                        String toPath = task.getOutput();

                        // 参数 HiveMeta 对象
                        HiveMeta hiveMeta = new TableHelper().generalHiveMeta(task, jobRunning);
                        log.debug("BitmapBuildingTask {} start.", task.getId());

                        // 2. do task
                        TaskState state = null;

                        //3. 更新元数据
                        new MetadataJob().completeTask(task.getId(), state,
                                new Date(System.currentTimeMillis()), toPath);

                        log.debug("BitmapBuildingTask {} finished.", task.getId());
                    }
                    break;
                case 1:
                    if (new TaskHelper().preTaskFinish(tasks, i)) {
                        // bulkload() && 构建Slice addSlice();
                        // 1. task输入参数获取
                        Task task = tasks.get(i);
                        task.setState(TaskState.RUNNING);
                        jobRunning.setLatestTask(task.getId());
                        String dbName = jobRunning.getDbName();
                        String tableName = jobRunning.getTableName();
                        String srcPath = tasks.get(i - 1).getOutput();

                        log.debug("BulkLoadTask {} start.", task.getId());

                        //2. do task
                        StorageConnector storageConnector = null;

                        String sink = new Metadata().getSlices(dbName, tableName).get(0).getSink();

                        try {
                            storageConnector.createBatchRecords("", sink, srcPath);
                        } catch (StorageException e) {
                            log.error("{}, Error bulk load to HBase!", task.getId());
                        }

                        Slice slice = null;
                        TaskState state = null;

                        //3. 更新元数据
                        new MetadataJob().completeTask(task.getId(), state,
                                new Date(System.currentTimeMillis()), task.getOutput());
                        log.debug("BulkLoadTask {} finished.", task.getId());

                        jobRunning.setState(JobState.SUCCESS);
                    }
                    break;

                default:

            }
            i = i % stepNum;
        }

        // 任务执行成功才从队列里面删除
        if (JobState.SUCCESS == jobRunning.getState()) {
            AbstractJob.PENDING_JOBS_QUEUE.remove(new MetadataJob().getJob(jobId));
        }

        return null;
    }


}
