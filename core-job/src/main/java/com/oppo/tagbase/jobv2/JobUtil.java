package com.oppo.tagbase.jobv2;

import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.Task;

import java.util.List;

import static java.lang.String.format;

/**
 * Created by wujianchao on 2020/2/28.
 */
public class JobUtil {

    public static String taskName(Job job, Task task) {
        return format("%s - step %d", job.getName(), task.getStep());
    }

    public static Task previousTask(List<Task> taskList, Task current) {
        byte step = current.getStep();
        if(step == 0) {
            return null;
        }
        return taskList.get(step - 1);
    }

    public static Task previousTask(Job job, Task current) {
        return previousTask(job.getTasks(), current);
    }
}
