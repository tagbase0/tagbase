package com.oppo.tagbase.job.util;

import com.oppo.tagbase.meta.obj.Task;
import com.oppo.tagbase.meta.obj.TaskState;

import java.util.List;

/**
 * Created by daikai on 2020/2/19.
 */
public class TaskHelper {

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
}
