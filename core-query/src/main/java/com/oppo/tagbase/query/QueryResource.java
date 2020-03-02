package com.oppo.tagbase.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.oppo.tagbase.query.common.IdGenerator;
import com.oppo.tagbase.query.node.OutputType;
import com.oppo.tagbase.query.node.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.Callable;

/**
 * @author huangfeng
 * @date 2020/2/7
 */

@Path("/tagbase/v1/")
public class QueryResource {

    private static Logger LOG = LoggerFactory.getLogger(QueryResource.class);

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

        String currThreadName = Thread.currentThread().getName();
        String id = null;
        try {

            Query query = jsonMapper.readValue(req.getInputStream(), Query.class);

            boolean isSync = query.getOutput() == OutputType.COUNT ? true : false;

            id = idGenerator.getNextId();

            String queryThreadName = String.format(
                    "%s[%s]",
                    currThreadName, id);
            Thread.currentThread().setName(queryThreadName);

            QueryExecution execution = queryExecutionFactory.create(id, query);

            execution.execute();

            if (isSync) {
                return QueryResponse.fillContent(queryManager.getResult(id));
            } else {
                return QueryResponse.fillContent(id);
            }
        } catch (Exception e) {
            queryManager.remove(id);
            LOG.error("query fail: ", e);
            return QueryResponse.error(e);
        } finally {
            Thread.currentThread().setName(currThreadName);
        }

    }

    @GET
    @Path("{queryId}/state")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public QueryResponse queryState(@PathParam("queryId") String queryId) {
        return tryAndReturn(() ->
                queryManager.queryState(queryId)
        );

    }

    @GET
    @Path("{queryId}/result")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public QueryResponse result(@QueryParam("queryId") String queryId) {
        return tryAndReturn(() ->
                queryManager.getResult(queryId)
        );
    }


    @DELETE
    @Path("cancel")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public QueryResponse cancel(@QueryParam("id") String id) {
        return tryAndReturn(() ->
                queryManager.cancel(id)
        );
    }


    @GET
    @Path("show")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public QueryResponse showQueries() {

        return QueryResponse.fillContent(queryManager.getQueries());
    }

    private static QueryResponse tryAndReturn(Callable task) {
        try {
            return QueryResponse.fillContent(task.call());
        } catch (Exception e) {
            return QueryResponse.error(e);
        }

    }
}
