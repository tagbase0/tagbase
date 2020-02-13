package com.oppo.tagbase.common.example.lifecycle;

import com.oppo.tagbase.guice.LifecycleStart;
import com.oppo.tagbase.guice.LifecycleStop;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class DataUpdater {

    @LifecycleStart
    public void start() {
        System.out.println("DataUpdater start");
    }

    @LifecycleStop
    public void stop() {
        System.out.println("DataUpdater stop");
    }

}
