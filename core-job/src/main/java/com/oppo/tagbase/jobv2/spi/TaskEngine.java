package com.oppo.tagbase.jobv2.spi;

import com.oppo.tagbase.common.guice.Extension;
import com.oppo.tagbase.jobv2.JobException;

/**
 * Created by wujianchao on 2020/3/2.
 */
@Extension(key = "tagbase.job.external.engine.type", defaultImpl = "spark")
public interface TaskEngine {

    String buildDict(DictTaskContext context) throws JobException;
    String buildData(DataTaskContext context) throws JobException;
    TaskStatus kill(String appId) throws JobException;
    TaskStatus status(String appId) throws JobException;

}
