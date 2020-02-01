package com.oppo.tagbase.query.http;

import com.google.inject.Singleton;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by wujianchao on 2020/1/15.
 */
@Path("/status/v1")
public class StatusResource {

    public StatusResource() {
        System.out.println("create StatusResource");
    }

    @GET
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response status(@Context final HttpServletRequest req) {
        return Response.ok("ok").build();
    }

    @GET
    @Path("/detail")
    public Response detail(@Context final HttpServletRequest req,
                           @QueryParam("p") @DefaultValue("world") String p) {
        return Response.ok("ok " + p).build();
    }

}
