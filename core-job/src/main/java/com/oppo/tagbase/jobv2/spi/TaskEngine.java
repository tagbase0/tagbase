package com.oppo.tagbase.jobv2.spi;

import com.oppo.tagbase.jobv2.JobException;
import com.oppo.tagbase.meta.obj.JobType;

import java.io.IOException;

/**
 * Created by liangjingya on 2020/2/20.
 */
public abstract class TaskEngine {

    public abstract String submitTask(HiveMeta hiveMeta, JobType type) throws JobException;

    public abstract TaskStatus getTaskStatus(String appId, JobType type) throws IOException, JobException;

}
