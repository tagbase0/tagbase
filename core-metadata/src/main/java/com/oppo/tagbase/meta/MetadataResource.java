package com.oppo.tagbase.meta;

import com.oppo.tagbase.meta.obj.Column;
import com.oppo.tagbase.meta.obj.DB;
import com.oppo.tagbase.meta.obj.Table;
import com.oppo.tagbase.meta.obj.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * RestFull API for
 *  1. creating db, table
 *  2. getting db, table details
 *
 * Created by wujianchao on 2020/2/5.
 */
@Path("/tagbase/v1/metadata")
public class MetadataResource {

    private Logger log = LoggerFactory.getLogger(getClass());

    @Inject
    private Metadata metadata;


    @GET
    @Path("/dbs")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listDb() {
        return Response.ok().entity(metadata.listDBs()).build();
    }

    @GET
    @Path("/{dbName}/tables")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listTable(@PathParam("dbName") @NotNull String dbName) {
        return Response.ok().entity(metadata.listTables(dbName)).build();
    }

    //TODO bind javax validation to Jersey
    @POST
    @Path("/db")
    @Produces(MediaType.APPLICATION_JSON)
    public Response addDb(@FormParam("dbName") @NotNull(message = "dbName is null")  String dbName,
                             @FormParam("desc") String desc) {
        metadata.addDb(dbName, desc);
        DB db = metadata.getDb(dbName);
        return Response.ok(db).build();
    }

    @POST
    @Path("/{dbName}/table")
    @Produces(MediaType.APPLICATION_JSON)
    public Response addTable(@PathParam("dbName") @NotNull(message = "dbName is null") String dbName,
                                @FormParam("tableName") @NotNull(message = "tableName is null") String tableName,
                                @FormParam("srcDb") @NotNull(message = "srcDb is null") String srcDb,
                                @FormParam("srcTable") @NotNull(message = "srcTable is null") String srcTable,
                                @FormParam("desc") String desc,
                                @FormParam("type") @NotNull(message = "type is null") TableType type,
                                @FormParam("columnList") @NotNull(message = "columnList is null") List<Column> columnList) {
        try {
            metadata.addTable(dbName,
                    tableName,
                    srcDb,
                    srcTable,
                    desc,
                    type,
                    columnList);
            Table table = metadata.getTable(dbName, tableName);
            return Response.ok(table).build();
        } catch (Exception e) {
            log.error("Failed to add Table " + tableName, e);
            //TODO
            return Response.ok().build();
        }
    }


}
