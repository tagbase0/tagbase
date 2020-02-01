package com.oppo.tagbase.example.lifecycle;

import com.oppo.tagbase.guice2.LifecycleBinding;
import com.oppo.tagbase.guice2.LifecycleStart;
import com.oppo.tagbase.guice2.LifecycleStop;

/**
 * Created by wujianchao on 2020/1/21.
 */
@LifecycleBinding
public class DataUpdater {

    @LifecycleStart
    public void xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx(){
        System.out.println("DataUpdater start");
    }

    @LifecycleStop
    public void stop(){
        System.out.println("DataUpdater stop");
    }

}
