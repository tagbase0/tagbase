package com.oppo.tagbase.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.oppo.tagbase.guice.GuiceInjectors;
import com.oppo.tagbase.guice.JacksonModule;
import com.oppo.tagbase.guice.JettyModule;
import com.oppo.tagbase.guice.Lifecycle;
import com.oppo.tagbase.guice.LifecycleModule;
import com.oppo.tagbase.guice.ValidatorModule;
import com.oppo.tagbase.module.QueryModule;
import com.oppo.tagbase.query.ComplexQuery;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

/**
 * Created by 71518 on 2020/2/7.
 */

@Path("/tagbase/v1/")
public class QueryResource {
    protected final ObjectMapper jsonMapper;

    @Inject
    public QueryResource(ObjectMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
        System.out.println("create queryResource");
    }

    @POST
    @Path("query")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response query(@Context final HttpServletRequest req) {

        try {
            ComplexQuery query = jsonMapper.readValue(req.getInputStream(),ComplexQuery.class);
            System.out.println(query);


        } catch (IOException e) {
            e.printStackTrace();
        }

        return Response.ok("ok").build();
    }


    public static void main(String[] args) {
        Injector ij = GuiceInjectors.makeInjector(
                new JettyModule(),
                new LifecycleModule(),
                new ValidatorModule(),
                new JacksonModule(),
                new QueryModule()
        );
        Lifecycle lifecycle = ij.getInstance(Lifecycle.class);
        lifecycle.start();
    }

}
