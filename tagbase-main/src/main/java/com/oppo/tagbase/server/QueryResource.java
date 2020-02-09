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
import com.oppo.tagbase.query.*;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

/**
 * Created by 71518 on 2020/2/7.
 */

@Path("/tagbase/v1/")
public class QueryResource {
    protected final ObjectMapper jsonMapper;
    private final QueryManager queryManager;
    private final QueryExecutionFactory queryExecutionFactory;

    @Inject
    public QueryResource(ObjectMapper jsonMapper, QueryManager queryManager,QueryExecutionFactory queryExecutionFactory) {
        this.jsonMapper = jsonMapper;
        this.queryManager = queryManager;
        this.queryExecutionFactory = queryExecutionFactory;
    }

    @POST
    @Path("query")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public QueryResponse query(@Context final HttpServletRequest req) {

        QueryResponse response = null;
        try {

            Query query = jsonMapper.readValue(req.getInputStream(), Query.class);

            QueryExecution execution = queryExecutionFactory.create(query);

            response = execution.execute();


        } catch (Exception e) {
            response = QueryResponse.error(e);
        }

        return response;
    }



    @POST
    @Path("cancel")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public QueryResponse cancel(@Context final HttpServletRequest req) {

        return null;
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
