package com.oppo.tagbase;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.oppo.tagbase.guice.ResourceModule;
import com.oppo.tagbase.guice2.GuiceInjectors;
import com.oppo.tagbase.guice2.JacksonModule;
import com.oppo.tagbase.guice2.PropsModule;
import com.oppo.tagbase.guice2.ResourceContainer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * Just for test.
 * <p>
 * Created by wujianchao on 2020/1/15.
 */
public class ServerMain {

    public static void main(String[] args) throws Exception {
        Injector ij = GuiceInjectors.makeInjector(
                new PropsModule(ImmutableList.of("tagbase.properties")),
                new JacksonModule(),
                new ResourceModule()
        );

        Server server = new Server(8080);

        ServletContextHandler handler = new ServletContextHandler();
        handler.setContextPath("/");
        handler.addServlet(new ServletHolder(ij.getInstance(ResourceContainer.class)), "/*");
        server.setHandler(handler);

        server.start();

//        JmxReporter reporter = JmxReporter.forRegistry(injector.getInstance(MetricRegistry.class)).build();
//        reporter.start();
    }
}
