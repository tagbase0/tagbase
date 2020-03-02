package com.oppo.tagbase.jobv2;

import com.oppo.tagbase.common.guice.LifecycleStart;
import com.oppo.tagbase.common.guice.LifecycleStop;
import com.oppo.tagbase.common.guice.Extension;

/**
 *
 * Created by wujianchao on 2020/2/26.
 */
@Extension(key = "tagbase.job.scheduler.type", defaultImpl = "singleton")
public interface Scheduler {

    @LifecycleStart
    void schedule();

    @LifecycleStop
    void shutdown();
}
