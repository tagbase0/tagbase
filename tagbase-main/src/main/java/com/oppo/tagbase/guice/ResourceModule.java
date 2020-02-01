package com.oppo.tagbase.guice;

import com.oppo.tagbase.guice2.Resources;
import com.oppo.tagbase.query.http.StatusResource;
import com.sun.jersey.guice.JerseyServletModule;

/**
 * Created by wujianchao on 2020/1/15.
 */
public class ResourceModule extends JerseyServletModule {

    @Override
    protected void configureServlets() {
        Resources.addResource(binder(), StatusResource.class);
    }

}
