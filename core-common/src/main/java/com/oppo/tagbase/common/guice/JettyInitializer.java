package com.oppo.tagbase.common.guice;

import com.google.inject.Inject;
import org.eclipse.jetty.server.Server;

/**
 * Created by wujianchao on 2020/2/2.
 */
public class JettyInitializer {

    private Server server;

    @Inject
    private void inject(Server server) {
        this.server = server;
    }

    @LifecycleStart
    public void start() throws Exception {
        server.start();
    }

    @LifecycleStop
    public void stop() throws Exception {
        server.stop();
    }

}
