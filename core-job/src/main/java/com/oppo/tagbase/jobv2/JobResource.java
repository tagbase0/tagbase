package com.oppo.tagbase.jobv2;

import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobState;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.time.LocalDateTime;

/**
 * Created by wujianchao on 2020/2/26.
 */
@Path("/tagbase/v1/job")
public class JobResource {

    @Inject
    private JobManager manager;

    @POST
    @Path("/build/dict")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Job buildDict(@FormParam("dataLowerTime") @NotNull(message = "lower time is null") LocalDateTime dataLowerTime,
                         @FormParam("dataUpperTime") @NotNull(message = "upper time is null") LocalDateTime dataUpperTime) {

        return manager.buildDict(dataLowerTime, dataUpperTime);
    }

    @POST
    @Path("/build")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Job build(@FormParam("dbName") @NotNull(message = "db name is null") String dbName,
                     @FormParam("tableName") @NotNull(message = "table name is null") String tableName,
                     @FormParam("dataLowerTime") @NotNull(message = "lower time is null") LocalDateTime dataLowerTime,
                     @FormParam("dataUpperTime") @NotNull(message = "upper time is null") LocalDateTime dataUpperTime) {

        return manager.build(dbName, tableName, dataLowerTime, dataUpperTime);
    }

    @GET
    @Path("/{jobId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public JobState jobStatus(@PathParam("jobId") @NotNull(message = "job id is null") String jobId) {
        return manager.getJobState(jobId);
    }

    @POST
    @Path("/rebuild")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Job rebuild(@FormParam("dbName") @NotNull(message = "db name is null") String dbName,
                       @FormParam("tableName") @NotNull(message = "table name is null") String tableName,
                       @FormParam("dataLowerTime") @NotNull(message = "lower time is null") LocalDateTime dataLowerTime,
                       @FormParam("dataUpperTime") @NotNull(message = "upper time is null") LocalDateTime dataUpperTime) {

        return manager.build(dbName, tableName, dataLowerTime, dataUpperTime);
    }

    @POST
    @Path("/resume/{jobId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Job resumeJob(@PathParam("jobId") @NotNull(message = "job id is null") String jobId) {

        return manager.resumeJob(jobId);
    }

    @POST
    @Path("/suspend/{jobId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Job suspendJob(@PathParam("jobId") @NotNull(message = "job id is null") String jobId) {

        return manager.suspendJob(jobId);
    }

    @POST
    @Path("/stop/{jobId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Job discardJob(@PathParam("jobId") @NotNull(message = "job id is null") String jobId) {

        return manager.discardJob(jobId);
    }

    @DELETE
    @Path("/{jobId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public void deleteJob(@PathParam("jobId") @NotNull(message = "job id is null") String jobId) {
        manager.deleteJob(jobId);
    }


    @GET
    @Path("/{jobId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Job getJob(@PathParam("jobId") @NotNull(message = "job id is null") String jobId) {

        return manager.getJob(jobId);
    }

    @GET
    @Path("/{dbName}/{tableName}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Job listJob(@PathParam("dbName") @NotNull String dbName,
                       @PathParam("dbName") @NotNull String tableName) {

        return manager.listJob(dbName, tableName);
    }

    @GET
    @Path("/{dbName}/{tableName}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Job listJob(@PathParam("dbName") @NotNull String dbName,
                       @PathParam("dbName") @NotNull String tableName,
                       @FormParam("startTime") @NotNull LocalDateTime startTime,
                       @FormParam("endTime") @NotNull LocalDateTime endTime) {

        return manager.listJob(dbName, tableName, startTime, endTime);
    }


}
