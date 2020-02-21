package com.oppo.tagbase.job;

import com.oppo.tagbase.job.util.IdGenerator;
import com.oppo.tagbase.job.util.TaskHelper;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.*;

import java.sql.Date;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by daikai on 2020/2/16.
 */
public class BitMapBuildJob extends Task implements Callable<Slice> {

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

        List<Task> tasks = jobRunning.getTasks();
        int stepNum = tasks.size();

        JobState jobState = jobRunning.getState();
        Table table = new Metadata().getTable(jobRunning.getDbName(), jobRunning.getTableName());

        //TODO 2020/2/16  分步骤执行
        // 当该 job 下所有子任务都执行成功，则循环结束
        for (int i = 0; jobState != JobState.SUCCESS; i++) {
            switch (i) {
                case 0:
                    // 仅仅当前任务的前置任务都正常执行成功，才开启这个任务
                    if (new TaskHelper().preTaskFinish(tasks, i)) {
                        // BitmapBuildingTask();
                        // 参数 反向字典hdfs路径, 标签 dbName 和 tableName, 以及生成的 Hfile 的存放路径
                        Task task = tasks.get(i);
                        String dbName = jobRunning.getDbName();
                        String tableName = jobRunning.getTableName();
                        Date dataLowerTime = jobRunning.getDataLowerTime();
                        Date dataUpperTime = jobRunning.getDataUpperTime();

                        String invertedDictPath = new MetadataJob().getJob(
                                new IdGenerator().nextQueryId("DictBuildJob", "yyyyMMdd")).
                                getTasks().get(0).getOutput();

                        String toPath = task.getOutput();

//                        Task2Engine.generateHfile(task.getId(), dbName, tableName,
//                                locationInvertedDict, toPath);

                        new MetadataJob().completeTask(task.getId(), task.getState(),
                                new Date(System.currentTimeMillis()), toPath);
                    }
                    break;
                case 1:
                    if (new TaskHelper().preTaskFinish(tasks, i)) {
                        // bulkload() && 构建Slice addSlice();
                        // 参数task0的输出路径
                        Task task = tasks.get(i);
                        String srcPath = tasks.get(i - 1).getOutput();
                        String desPath = task.getOutput();

                        Slice slice = new Slice();


                        new MetadataJob().completeTask(task.getId(), task.getState(),
                                new Date(System.currentTimeMillis()), task.getOutput());
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
