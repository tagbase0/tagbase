package com.oppo.tagbase.job_v2;

import java.io.IOException;

/**
 *
 * External executors such as Hadoop, Fink, Spark etc.
 *
 * Created by wujianchao on 2020/2/26.
 */
public abstract class ExternalExecutor<C> implements Executor<ExternalExecutable<C>> {

    /**
     * get external task execution status.
     *
     * @param externalAppId such as spark app id.
     */
    protected abstract ExternalTaskStatus getTaskStatus(String externalAppId) throws IOException, JobException;

    @Override
    public void perform(ExternalExecutable<C> task) {

    }
}
