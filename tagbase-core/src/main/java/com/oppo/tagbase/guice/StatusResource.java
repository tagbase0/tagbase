package com.oppo.tagbase.guice;

import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
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
                           @NotNull @QueryParam("p")  String p) {
        return Response.ok("detailed status of " + p).build();
    }

}
