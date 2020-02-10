package com.oppo.tagbase.storage.lifecycle;

import com.google.inject.Inject;
import com.oppo.tagbase.guice.LifecycleStart;
import com.oppo.tagbase.guice.LifecycleStop;
import com.oppo.tagbase.storage.connector.StorageConnector;

/**
 * Created by liangjingya on 2020/2/10.
 */
public class ConnectorLifecycle {

    @Inject
    private StorageConnector connector;

    @LifecycleStart
    public void start() {
        System.out.println("storage module start!");
        connector.initConnector();

    }

    @LifecycleStop
    public void stop() {
        System.out.println("storage module stop!");
        connector.destroyConnector();
    }

}
