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
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<DB> listDb() {
        return metadata.listDBs();
    }

    @GET
    @Path("/{dbName}/tables")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<Table> listTable(@PathParam("dbName") @NotNull String dbName) {
        return metadata.listTables(dbName);
    }

    //TODO bind javax validation to Jersey
    @POST
    @Path("/db")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public DB addDb(@FormParam("dbName") @NotNull(message = "dbName is null")  String dbName,
                             @FormParam("desc") String desc) {
        metadata.addDb(dbName, desc);
        return metadata.getDb(dbName);
    }

    @POST
    @Path("/{dbName}/table")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Table addTable(@PathParam("dbName") @NotNull(message = "dbName is null") String dbName,
                                @FormParam("tableName") @NotNull(message = "tableName is null") String tableName,
                                @FormParam("srcDb") @NotNull(message = "srcDb is null") String srcDb,
                                @FormParam("srcTable") @NotNull(message = "srcTable is null") String srcTable,
                                @FormParam("desc") String desc,
                                @FormParam("type") @NotNull(message = "type is null") TableType type,
                                @FormParam("srcType") @NotNull(message = "type is null") String srcType,
                                @FormParam("columnList") @NotNull(message = "columnList is null") List<Column> columnList) {
        metadata.addTable(dbName,
                tableName,
                srcDb,
                srcTable,
                desc,
                type,
                srcType,
                columnList);
        return metadata.getTable(dbName, tableName);
    }


}
