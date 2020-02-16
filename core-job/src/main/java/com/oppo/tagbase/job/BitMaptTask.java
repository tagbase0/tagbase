package com.oppo.tagbase.job;

import com.oppo.tagbase.meta.obj.Slice;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by daikai on 2020/2/16.
 */
public class BitMaptTask implements Callable<Slice> {

    private String taskId;
    private String appId;
    private long startTime;
    private long endTime;
    private State taskState;
    int step;
    private String jobId;

    public BitMaptTask(String jobId) {
        this.jobId = jobId;
    }

    public BitMaptTask(String taskId, String appId, long startTime, long endTime,
                       State taskState, int step, String jobId) {
        this.taskId = taskId;
        this.appId = appId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.taskState = taskState;
        this.step = step;
        this.jobId = jobId;
    }

    @Override
    public Slice call() throws Exception {
        List<BitMaptTask> listBuildTasks = new BitMapJob().getAllBuildTasks(jobId);
        int stepNum = listBuildTasks.size();
        //TODO 2020/2/16  分步骤执行
        for(int i = 0; i < stepNum; i++){
            switch(i){
                case 0 :
                    //BitmapBuildingTask();
                    break;
                case 1 :
                    //loadbulk()
                    break;
                case 2 :
                    //addSlice()
                    break;

                default :

            }
        }
        return null;
    }
}
