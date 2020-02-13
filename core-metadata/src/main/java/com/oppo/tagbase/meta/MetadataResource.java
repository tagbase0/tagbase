package com.oppo.tagbase.meta;

import com.oppo.tagbase.meta.obj.Column;
import com.oppo.tagbase.meta.obj.DB;
import com.oppo.tagbase.meta.obj.Table;
import com.oppo.tagbase.meta.obj.TableType;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * Created by wujianchao on 2020/2/5.
 */
@Path("/tagbase/metadata/v1")
public class MetadataResource {

    @Inject
    private Metadata metadata;


    @GET
    @Path("/dbs")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listDb() {
        return Response.ok().entity(metadata.listDBs()).build();
    }

    @GET
    @Path("/tables")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listTable(@QueryParam("dbName") @NotNull String dbName) {
        return Response.ok().entity(metadata.listTables(dbName)).build();
    }

    //TODO bind javax validation to Jersey
    @POST
    @Path("/db")
    @Produces(MediaType.APPLICATION_JSON)
    public Response createDb(@QueryParam("dbName") @NotNull String dbName,
                             @QueryParam("desc") String desc) {
        metadata.createDB(dbName, desc);
        DB db = metadata.getDb(dbName);
        return Response.ok(db).build();
    }

    @POST
    @Path("/table")
    @Produces(MediaType.APPLICATION_JSON)
    public Response createTable(@QueryParam("dbName") @NotNull String dbName,
                                @QueryParam("tableName") @NotNull String tableName,
                                @QueryParam("srcDb") @NotNull String srcDb,
                                @QueryParam("srcTable") @NotNull String srcTable,
                                @QueryParam("desc") String desc,
                                @QueryParam("type") @NotNull TableType type,
                                @QueryParam("columnList") @NotNull List<Column> columnList) {
        metadata.createTable(dbName,
                tableName,
                srcDb,
                srcTable,
                desc,
                type,
                columnList);
        Table table = metadata.getTable(dbName, tableName);
        return Response.ok(table).build();
    }


}
