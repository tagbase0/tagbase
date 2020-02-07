package com.oppo.tagbase.guice;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wujianchao on 2020/1/21.
 */
public class JettyModule extends JerseyServletModule {

    private Logger log = LoggerFactory.getLogger(getClass());


    //configureServlets
    @Override
    protected void configureServlets() {
        Binder binder = binder();
        ConfBind.bind(binder, "tagbase.server", JettyConfig.class);
        binder.bind(GuiceContainer.class).to(ResourceContainer.class);
        Lifecycle.registerInstance(binder, JettyInitializer.class);
    }

    @Provides
    @Singleton
    private Server getJettyServer(JettyConfig config, GuiceContainer container) {

        System.out.println("new jetty server");

        QueuedThreadPool pool = new QueuedThreadPool(
                config.getNumThreads(),
                Math.max(1, config.getNumThreads() / 2));

        pool.setName("jetty");

        Server server = new Server(pool);

        // adding welcome message.
        server.addLifeCycleListener(new AbstractLifeCycle.AbstractLifeCycleListener() {
            @Override
            public void lifeCycleStarted(LifeCycle event) {
                welcome();
            }
        });

        ServerConnector connector = new ServerConnector(server);
        connector.setPort(config.getPort());
        server.setConnectors(new Connector[]{connector});

        ServletContextHandler handler = new ServletContextHandler();
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(container), "/*");
        server.setHandler(handler);

        return server;
    }

    private void welcome() {
        String welcome = "Tagbase is ready to work!";
        log.info(welcome);
    }

}
