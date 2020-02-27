package com.oppo.tagbase.job.engine;

import com.oppo.tagbase.job.engine.exception.JobException;
import com.oppo.tagbase.job.engine.obj.HiveMeta;
import com.oppo.tagbase.job.engine.obj.JobType;
import com.oppo.tagbase.job.engine.obj.TaskMessage;

/**
 * Created by liangjingya on 2020/2/20.
 */
public abstract class TaskEngine {

    public abstract String submitTask(HiveMeta hiveMeta, JobType type) throws JobException;

    public abstract TaskMessage getTaskStatus(String appid, JobType type) throws JobException;

}
