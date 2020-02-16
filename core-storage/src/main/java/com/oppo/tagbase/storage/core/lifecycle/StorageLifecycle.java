package com.oppo.tagbase.storage.core.lifecycle;

import com.google.inject.Inject;
import com.oppo.tagbase.common.guice.LifecycleStart;
import com.oppo.tagbase.common.guice.LifecycleStop;
import com.oppo.tagbase.storage.core.connector.StorageConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liangjingya on 2020/2/10.
 */
public class StorageLifecycle {

    @Inject
    private StorageConnector connector;

    private Logger log = LoggerFactory.getLogger(StorageLifecycle.class);

    @LifecycleStart
    public void start() {
        log.info("storage module start!");
        connector.init();

    }

    @LifecycleStop
    public void stop() {
        log.info("storage module stop!");
        connector.destroy();
    }

}
