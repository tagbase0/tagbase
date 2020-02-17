package com.oppo.tagbase.job;

import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.*;

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

    /**
     * 检查这个任务的前置任务是否正常执行成功
     */
    public boolean preTaskFinish(List<Task> tasks, int step) {

        for (int i = 0; i < step; i++) {
            if (TaskState.SUCCESS != tasks.get(i).getState()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Slice call() {

        Job jobRunning = new MetadataJob().getJob(jobId);

        List<Task> tasks = jobRunning.getTasks();
        int stepNum = tasks.size();

        JobState jobState = jobRunning.getState();

        //TODO 2020/2/16  分步骤执行
        // 当该 job 下所有子任务都执行成功，则循环结束
        for (int i = 0; jobState != JobState.SUCCESS; i++) {
            switch (i) {
                case 0:
                    // 仅仅当前任务的前置任务都正常执行成功，才开启这个任务
                    if (preTaskFinish(tasks, i)) {
                        // BitmapBuildingTask();
                    }
                    break;
                case 1:
                    if (preTaskFinish(tasks, i)) {
                        // bulkload();
                    }
                    break;
                case 2:
                    if (preTaskFinish(tasks, i)) {
                        //addSlice()

                        // 所有子任务都success, 则将这个job标记为success
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
