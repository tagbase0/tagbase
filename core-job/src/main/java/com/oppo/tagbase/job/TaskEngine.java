package com.oppo.tagbase.job;

import com.oppo.tagbase.job.exception.JobException;
import com.oppo.tagbase.job.obj.HiveMeta;
import com.oppo.tagbase.job.obj.TaskMessage;
import com.oppo.tagbase.meta.obj.JobType;

import java.io.IOException;

/**
 * Created by liangjingya on 2020/2/20.
 */
public abstract class TaskEngine {

    public abstract String submitTask(HiveMeta hiveMeta, JobType type) throws JobException;

    public abstract TaskMessage getTaskStatus(String appid, JobType type) throws IOException, JobException;

}
