package com.oppo.tagbase.common.guice;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.servlet.ServletContainer;

import java.util.Set;

/**
 * Created by wujianchao on 2020/1/15.
 */
@Singleton
public class ResourceContainer extends ServletContainer {

    @Inject
    public ResourceContainer(@Resource Set<Class<?>> resources) {
        super(new ResourceConfig(resources).property(ServerProperties.BV_SEND_ERROR_IN_RESPONSE,true));
        System.out.println(resources);

    }

}
