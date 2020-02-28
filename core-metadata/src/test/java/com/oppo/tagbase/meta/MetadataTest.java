package com.oppo.tagbase.meta;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.inject.Injector;
import com.oppo.tagbase.common.guice.ExampleGuiceInjectors;
import com.oppo.tagbase.common.guice.PropsModule;
import com.oppo.tagbase.common.guice.ValidatorModule;
import com.oppo.tagbase.meta.connector.MetaStoreConnectorConfig;
import com.oppo.tagbase.meta.obj.Column;
import com.oppo.tagbase.meta.obj.ColumnType;
import com.oppo.tagbase.meta.obj.Slice;
import com.oppo.tagbase.meta.obj.SliceStatus;
import com.oppo.tagbase.meta.obj.Table;
import com.oppo.tagbase.meta.obj.TableType;
import com.oppo.tagbase.meta.type.DataType;
import org.junit.Assert;
import org.junit.Before;


import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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

    public void initSchemaTest() {
        metadata.initSchema();
    }

    /*-------------Metadata DDL part--------------*/

    public void addDb() {
        metadata.addDb("test_db", "For test");
        Assert.assertEquals("test_db", metadata.getDb("test_db").getName());
    }


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

    public void getTable() {
        Table table = metadata.getTable("test_db", "test_table_tag_city");

        Table tableAction = metadata.getTable("test_db",
                "test_table_action");

        Assert.assertEquals("test_db_hive_srcDb_tag", table.getSrcDb());
        Assert.assertEquals("test_table_hive_srcTable_tag", table.getSrcTable());

        Assert.assertEquals("test_db_hive_srcDb_action", tableAction.getSrcDb());
        Assert.assertEquals("test_table_hive_srcTable_action", tableAction.getSrcTable());
    }


    public void addSlice() {
        Slice sliceCity = new Slice();
        sliceCity.setStartTime(LocalDateTime.parse("2018-06-01 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        sliceCity.setEndTime(LocalDateTime.parse("2018-06-02 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        sliceCity.setTableId(1);
        sliceCity.setSink("/tagbase_tag_city_01");
        sliceCity.setShardNum(1);
        sliceCity.setSrcSizeMb(50);
        sliceCity.setSrcCount(500);
        sliceCity.setSinkSizeMb(50);
        sliceCity.setSinkCount(500);
        sliceCity.setStatus(SliceStatus.BUILDING);
        metadata.addSlice(sliceCity);

        Slice sliceCity1 = new Slice();
        sliceCity1.setStartTime(LocalDateTime.parse("2018-06-02 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        sliceCity1.setEndTime(LocalDateTime.parse("2018-06-03 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        sliceCity1.setTableId(1);
        sliceCity1.setSink("/tagbase_tag_city_02");
        sliceCity1.setShardNum(1);
        sliceCity1.setSrcSizeMb(50);
        sliceCity1.setSrcCount(500);
        sliceCity1.setSinkSizeMb(50);
        sliceCity1.setSinkCount(500);
        sliceCity1.setStatus(SliceStatus.BUILDING);
        metadata.addSlice(sliceCity1);

        Slice sliceAction = new Slice();
        sliceAction.setStartTime(LocalDateTime.parse("2018-06-01 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        sliceAction.setEndTime(LocalDateTime.parse("2018-06-02 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        sliceAction.setTableId(2);
        sliceAction.setSink("/tagbase_action_01");
        sliceAction.setShardNum(1);
        sliceAction.setSrcSizeMb(60);
        sliceAction.setSrcCount(600);
        sliceAction.setSinkSizeMb(60);
        sliceAction.setSinkCount(600);
        sliceAction.setStatus(SliceStatus.BUILDING);

        metadata.addSlice(sliceAction);

        Slice sliceAction1 = new Slice();
        sliceAction1.setStartTime(LocalDateTime.parse("2018-06-02 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        sliceAction1.setEndTime(LocalDateTime.parse("2018-06-03 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        sliceAction1.setTableId(2);
        sliceAction1.setSink("/tagbase_action_02");
        sliceAction1.setShardNum(1);
        sliceAction1.setSrcSizeMb(60);
        sliceAction1.setSrcCount(600);
        sliceAction1.setSinkSizeMb(60);
        sliceAction1.setSinkCount(600);
        sliceAction1.setStatus(SliceStatus.BUILDING);

        metadata.addSlice(sliceAction1);

    }


    public void updateSliceStatus() {

        metadata.updateSliceStatus(3, 2, SliceStatus.READY);
        metadata.updateSliceStatus(4, 2, SliceStatus.READY);
        Assert.assertEquals("/tagbase_action_01", metadata.getSlice("/tagbase_action_01").getSink());

    }


    public void updateSliceSinkStatistics() {

        metadata.updateSliceSinkStatistics(3, 80, 760);
        Assert.assertEquals(760, metadata.getSlice("/tagbase_action_01").getSinkCount());

    }

    /*-------------Metadata API for query--------------*/

    public void getSlices() {

        System.out.println(metadata.getSlices("test_db", "test_table_tag_city"));
        System.out.println(metadata.getSlices("test_db", "test_table_action"));
        Assert.assertEquals(2, metadata.getSlices("test_db", "test_table_action").size());
    }


    public void getSlicesFilter() {
        RangeSet<LocalDateTime> range = TreeRangeSet.create();
        range.add(Range.closed(LocalDateTime.parse("2018-06-01 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                LocalDateTime.parse("2018-06-02 10:12:05", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));

        System.out.println(range);
        for(Slice slice : metadata.getSlices("test_db", "test_table_action", range)){
            System.out.println(slice);
        }

        Assert.assertEquals(1, metadata.getSlices("test_db", "test_table_action", range).size());

    }

    
    public void getSlicesGT() {
    }

    
    public void getSlicesGE() {
    }

    
    public void getSlicesLT() {
    }

    
    public void getSlicesLE() {
    }

    
    public void getSlicesBetween() {
    }


    /*-------------Metadata API for checking status--------------*/


    public void getDb() {
        Assert.assertEquals("For test", metadata.getDb("test_db").getDesc());
    }
}
