package com.oppo.tagbase.jobv2;

import com.oppo.tagbase.common.guice.LifecycleStart;
import com.oppo.tagbase.common.guice.LifecycleStop;

/**
 *
 * Created by wujianchao on 2020/2/26.
 */
public interface Scheduler<T extends Executable> {

    @LifecycleStart
    void schedule();

    @LifecycleStop
    void shutdown();
}
