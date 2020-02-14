package com.oppo.tagbase.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

import com.oppo.tagbase.query.*;
import com.oppo.tagbase.query.node.Query;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

/**
 * @author huangfeng
 * @date 2020/2/7
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


    @GET
    @Path("cancel")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public QueryResponse showQueries() {

        return null;
    }






}
