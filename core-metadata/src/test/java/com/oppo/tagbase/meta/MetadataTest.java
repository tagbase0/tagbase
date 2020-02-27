package com.oppo.tagbase.meta;

import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.oppo.tagbase.common.guice.ExampleGuiceInjectors;
import com.oppo.tagbase.common.guice.PropsModule;
import com.oppo.tagbase.common.guice.ValidatorModule;
import com.oppo.tagbase.meta.connector.MetaStoreConnectorConfig;
import com.oppo.tagbase.meta.obj.*;
import com.oppo.tagbase.meta.type.DataType;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wujianchao on 2020/2/27.
 */
public class MetadataTest {

    Metadata metadata;

    @Before
    public void setup() {
        Injector injector = ExampleGuiceInjectors.makeInjector(
                new ValidatorModule(),
                new PropsModule(Lists.newArrayList("tagbase.properties")),
                new MetadataModule()
        );

        MetaStoreConnectorConfig c = injector.getInstance(MetaStoreConnectorConfig.class);

        metadata =injector.getInstance(Metadata.class);
    }

    /*-------------Metadata initialization part--------------*/
    @Test
    public void initSchemaTest() {
        metadata.initSchema();
    }

    /*-------------Metadata DDL part--------------*/
    @Test
    public void addDb() {
        metadata.addDb("test_db", "For test");
    }

    @Test
    public void addTable() {

        // tag - city test
        String dbName = "test_db";
        String tableName = "test_table_tag_city";
        String srcDb = "test_db_hive_srcDb_tag";
        String srcTable= "test_table_hive_srcTable_tag";
        String desc = "For test table tag";
        TableType type = TableType.TAG;

        List<Column> columnList = new ArrayList<>();

        Column columnImei = new Column();
        columnImei.setSrcName("imei");
        columnImei.setName("imei");
        columnImei.setType(ColumnType.BITMAP_COLUMN);
        columnImei.setDesc("For test column imei");
        columnImei.setDataType(DataType.STRING);

        Column columnSlice = new Column();
        columnSlice.setSrcName("dayno");
        columnSlice.setName("dayno");
        columnSlice.setType(ColumnType.SLICE_COLUMN);
        columnSlice.setDesc("For test column dayno");
        columnSlice.setDataType(DataType.STRING);

        Column columnDim0 = new Column();
        columnDim0.setSrcName("city");
        columnDim0.setName("city");
        columnDim0.setType(ColumnType.DIM_COLUMN);
        columnDim0.setDesc("For test column city");
        columnDim0.setDataType(DataType.STRING);

        columnList.add(columnImei);
        columnList.add(columnSlice);
        columnList.add(columnDim0);

        metadata.addTable(dbName, tableName, srcDb, srcTable, desc, type, columnList);


        // action - app_action_version  test
        String dbName1 = "test_db";
        String tableName1 = "test_table_action";
        String srcDb1 = "test_db_hive_srcDb_action";
        String srcTable1= "test_table_hive_srcTable_action";
        String desc1 = "For test table action";
        TableType type1 = TableType.ACTION;

        List<Column> columnList1 = new ArrayList<>();

        Column columnImei1 = new Column();
        columnImei1.setSrcName("imei");
        columnImei1.setName("imei");
        columnImei1.setType(ColumnType.BITMAP_COLUMN);
        columnImei1.setDesc("For test column imei");
        columnImei1.setDataType(DataType.STRING);

        Column columnSlice1 = new Column();
        columnSlice1.setSrcName("dayno");
        columnSlice1.setName("dayno");
        columnSlice1.setType(ColumnType.SLICE_COLUMN);
        columnSlice1.setDesc("For test column dayno");
        columnSlice1.setDataType(DataType.STRING);

        Column columnDim00 = new Column();
        columnDim00.setSrcName("app");
        columnDim00.setName("app");
        columnDim00.setType(ColumnType.DIM_COLUMN);
        columnDim00.setDesc("For test column action table app");
        columnDim00.setDataType(DataType.STRING);

        Column columnDim01 = new Column();
        columnDim01.setSrcName("action");
        columnDim01.setName("action");
        columnDim01.setType(ColumnType.DIM_COLUMN);
        columnDim01.setDesc("For test column action table action");
        columnDim01.setDataType(DataType.STRING);

        Column columnDim02 = new Column();
        columnDim02.setSrcName("version");
        columnDim02.setName("version");
        columnDim02.setType(ColumnType.DIM_COLUMN);
        columnDim02.setDesc("For test column action table version");
        columnDim02.setDataType(DataType.STRING);

        columnList1.add(columnImei);
        columnList1.add(columnSlice);
        columnList1.add(columnDim00);
        columnList1.add(columnDim01);
        columnList1.add(columnDim02);

        metadata.addTable(dbName1, tableName1, srcDb1, srcTable1, desc1, type1, columnList1);

    }

    /*-------------Metadata API for data building--------------*/
    @Test
    public void getTable() {
        Table table = metadata.getTable("test_db", "test_table_tag_city");

        Table tableAction = metadata.getTable("test_db",
                "test_table_action");

        System.out.println("table: "+ table);
        System.out.println("tableAction: "+ tableAction);
    }


    @Test
    public void addSlice() throws ParseException {
        Slice sliceCity = new Slice();
        sliceCity.setStartTime(new Date(new SimpleDateFormat("yyyyMMdd").parse("20200221").getTime()));
        sliceCity.setEndTime(new Date(new SimpleDateFormat("yyyyMMdd").parse("20200222").getTime()));
        sliceCity.setTableId(1);
        sliceCity.setSink("/tagbase_tag_city");
        sliceCity.setShardNum(1);
        sliceCity.setSrcSizeMb(50);
        sliceCity.setSrcCount(500);
        sliceCity.setSinkSizeMb(50);
        sliceCity.setSinkCount(500);
        sliceCity.setStatus(SliceStatus.BUILDING);
        metadata.addSlice(sliceCity);

        Slice sliceAction = new Slice();
        sliceAction.setStartTime(new Date(new SimpleDateFormat("yyyyMMdd").parse("20200221").getTime()));
        sliceAction.setEndTime(new Date(new SimpleDateFormat("yyyyMMdd").parse("20200222").getTime()));
        sliceAction.setTableId(2);
        sliceAction.setSink("/tagbase_action");
        sliceAction.setShardNum(1);
        sliceAction.setSrcSizeMb(60);
        sliceAction.setSrcCount(600);
        sliceAction.setSinkSizeMb(60);
        sliceAction.setSinkCount(600);
        sliceAction.setStatus(SliceStatus.BUILDING);

        metadata.addSlice(sliceAction);

    }

    @Test
    public void updateSliceStatus() {

        metadata.updateSliceStatus(1, 1, SliceStatus.READY);
    }

    @Test
    public void updateSliceSinkStatistics() {

        metadata.updateSliceSinkStatistics(1, 80, 760);
    }

    /*-------------Metadata API for query--------------*/
    @Test
    public void getSlices() {

        System.out.println(metadata.getSlices("test_db", "test_table_tag_city"));
    }

    @Test
    public void getSlices1() {
    }

    @Test
    public void getSlicesGT() {
    }

    @Test
    public void getSlicesGE() {
    }

    @Test
    public void getSlicesLT() {
    }

    @Test
    public void getSlicesLE() {
    }

    @Test
    public void getSlicesBetween() {
    }


}
