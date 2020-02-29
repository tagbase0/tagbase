package com.oppo.tagbase.jobv2.skeleton;

import com.oppo.tagbase.jobv2.JobException;

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
    public void submit(ExternalExecutable<C> task) throws IOException {
        String appId = submitInternal(task);
        ExternalTaskStatus status = null;

        while (!(status = getTaskStatus(appId)).isDone()) {
            status = getTaskStatus(appId);
        }


    }

    public String submitInternal(ExternalExecutable<C> task) {
        return null;
    }


}
