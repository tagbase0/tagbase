package com.oppo.tagbase.job.engine;

import com.oppo.tagbase.job.engine.exception.JobException;
import com.oppo.tagbase.job.engine.obj.HiveMeta;
import com.oppo.tagbase.job.engine.obj.JobMessage;
import com.oppo.tagbase.job.engine.obj.JobType;
import java.io.IOException;

/**
 * Created by liangjingya on 2020/2/20.
 */
public abstract class AbstractExecutable {

    public abstract String submitJob(HiveMeta hiveMeta, JobType type) throws JobException;

    public abstract JobMessage getJobStatus(String appid, JobType type) throws IOException, JobException;

}
