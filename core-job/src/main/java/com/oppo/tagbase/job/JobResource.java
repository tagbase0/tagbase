package com.oppo.tagbase.job;

import com.oppo.tagbase.meta.MetadataJob;
import com.oppo.tagbase.meta.obj.JobState;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by daikai on 2020/2/22.
 */

@Path("/tagbase/job/v1")
public class JobResource {

    @Inject
    private DictJob dictJob;
    @Inject
    private BitMapJob bitMapJob;
    @Inject
    private MetadataJob metadataJob;


    @POST
    @Path("/build_dict")
    @Produces(MediaType.APPLICATION_JSON)
    public Response buildDict(@QueryParam("dbName") @NotNull String dbName,
                              @QueryParam("tableName") @NotNull String tableName) {

        dictJob.buildDict(dbName, tableName);
        return null;
    }

    @POST
    @Path("/build_data")
    @Produces(MediaType.APPLICATION_JSON)
    public Response buildDict(@QueryParam("dbName") @NotNull String dbName,
                              @QueryParam("tableName") @NotNull String tableName,
                              @QueryParam("dataLowerDate") @NotNull String dataLowerDate,
                              @QueryParam("dataUpperDate") @NotNull String dataUpperDate) {

        String jobId = bitMapJob.buildData(dbName, tableName, dataLowerDate, dataUpperDate);

        return Response.ok(jobId).build();
    }

    @POST
    @Path("/job_state")
    @Produces(MediaType.APPLICATION_JSON)
    public Response jobState(@QueryParam("jobId") @NotNull String jobId) {

        JobState jobState = metadataJob.getJob(jobId).getState();
        return Response.ok(jobState).build();
    }

}
