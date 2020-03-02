package com.oppo.tagbase.query.mock;


import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.obj.*;
import com.oppo.tagbase.meta.type.DataType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author huangfeng
 * @date 2020/2/25 19:25
 */

public class MetadataMock extends Metadata {

    Map<String, DB> dbs;

    Map<String, Map<String, Table>> dbTables;

    public MetadataMock() {
        dbs = new HashMap<>();
        dbTables = new HashMap<>();
    }

    public static Metadata mockMetadata() {
        String dbName = "tagbase";
        MetadataMock mockMetadata = new MetadataMock();
        mockMetadata.addDb(dbName, "tagbase");

        // table1  province
        mockMetadata.addTable(dbName, createProvinceTable());
        // table2 behavior
        mockMetadata.addTable(dbName, createBehaviorTable());

        return mockMetadata;
    }

    private static Table createBehaviorTable() {
        Column column1 = createNewColumn("dayno", ColumnType.SLICE_COLUMN, DataType.DATE);
        Column column2 = createNewColumn("app", ColumnType.DIM_COLUMN, DataType.STRING);
        Column column3 = createNewColumn("version", ColumnType.DIM_COLUMN, DataType.STRING);
        Column column4 = createNewColumn("behavior", ColumnType.DIM_COLUMN, DataType.STRING);
        Column column5 = createNewColumn("metric", ColumnType.BITMAP_COLUMN, DataType.BITMAP);
        return createNewTable("behavior", TableType.ACTION,Arrays.asList(column1, column2, column3, column4, column5));
    }

    private static Table createProvinceTable() {

        Column dimCol = createNewColumn("province", ColumnType.DIM_COLUMN, DataType.STRING);
        Column metricCol = createNewColumn("metric", ColumnType.DIM_COLUMN, DataType.STRING);

        return createNewTable("province", TableType.TAG,Arrays.asList(dimCol, metricCol));
    }

    private static Table createNewTable(String tableName, TableType tableType, List<Column> columns) {
        Table table = new Table();
        table.setName(tableName);
        table.setColumns(columns);
        table.setType(tableType);
        return table;
    }

    private static Column createNewColumn(String colName, ColumnType columnType, DataType dataType) {
        Column column = new Column();
        column.setName(colName);
        column.setType(columnType);
        column.setDataType(dataType);
        return column;
    }


    /*-------------Metadata DDL part--------------*/

    public void addDb(String dbName, String desc) {
        DB db = new DB();
        db.setDesc(desc);
        db.setName(dbName);
        dbs.put(dbName, db);
        dbTables.put(dbName,new HashMap<>());
    }


    public void addTable(String dbName,
                         Table table) {
        dbTables.get(dbName).put(table.getName(), table);

    }

    /*-------------Metadata API for data building--------------*/

    public Table getTable(String dbName, String tableName) {
        return dbTables.get(dbName).get(tableName);
    }

    /*-------------Metadata API for checking status--------------*/

    public DB getDb(String dbName) {
        return dbs.get(dbName);
    }

}
