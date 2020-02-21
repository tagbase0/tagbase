package com.oppo.tagbase.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.oppo.tagbase.query.*;
import com.oppo.tagbase.query.node.OutputType;
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
    private final IdGenerator idGenerator;

    @Inject
    public QueryResource(ObjectMapper jsonMapper, QueryManager queryManager, QueryExecutionFactory queryExecutionFactory, IdGenerator idGenerator) {
        this.jsonMapper = jsonMapper;
        this.idGenerator = idGenerator;
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
            boolean isSync = query.getOutput() == OutputType.COUNT ? true : false;
            String id = idGenerator.getNextId();
            QueryExecution execution = queryExecutionFactory.create(id, query);
            execution.execute();

            if (isSync) {
                return execution.getOutput();
            } else {
                return QueryResponse.queryId(id);
            }


        } catch (Exception e) {
            response = QueryResponse.error(e);
        }

        return response;
    }


    @GET
    @Path("cancel")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public QueryResponse cancel(@QueryParam("id") String id) {

        queryManager.cancelIfExist(id);
        return null;
    }


    @GET
    @Path("show")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public QueryResponse showQueries() {

        return null;
    }


}
