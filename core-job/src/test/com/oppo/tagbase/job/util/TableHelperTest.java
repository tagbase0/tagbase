package com.oppo.tagbase.job.util;

import com.oppo.tagbase.meta.obj.Column;
import com.oppo.tagbase.meta.obj.ColumnType;
import com.oppo.tagbase.meta.obj.Table;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;


/**
 * Created by daikai on 2020/2/24.
 */
public class TableHelperTest {
    static Table table;

    {
        table = new Table();
        long tableId = 2143243312L;
        table.setId(tableId);
        table.setName("test");

        Column columnImei = new Column();
        columnImei.setTableId(tableId);
        columnImei.setType(ColumnType.BITMAP_COLUMN);
        columnImei.setSrcName("imei");
        columnImei.setName("tag_imei");

        Column columnSlice = new Column();
        columnSlice.setTableId(tableId);
        columnSlice.setType(ColumnType.SLICE_COLUMN);
        columnSlice.setSrcName("dayno");

        Column columnDim0 = new Column();
        columnDim0.setTableId(tableId);
        columnDim0.setType(ColumnType.DIM_COLUMN);
        columnDim0.setSrcName("app");
        columnDim0.setIndex(0);

        Column columnDim1 = new Column();
        columnDim1.setTableId(tableId);
        columnDim1.setType(ColumnType.DIM_COLUMN);
        columnDim1.setSrcName("action");
        columnDim1.setIndex(1);

        Column columnDim2 = new Column();
        columnDim2.setTableId(tableId);
        columnDim2.setType(ColumnType.DIM_COLUMN);
        columnDim2.setSrcName("version");
        columnDim2.setIndex(2);


        List<Column> columnList = new ArrayList<>();
        columnList.add(columnImei);
        columnList.add(columnSlice);
        columnList.add(columnDim0);
        columnList.add(columnDim1);
        columnList.add(columnDim2);

        table.setColumns(columnList);
    }


    @Test
    public void getTableDimColumns() {

        Assert.assertEquals(new TableHelper().getTableDimColumns(table).toString(),
                "[app, action, version]");
    }

    @Test
    public void getTableImeiColumns() {

        Assert.assertEquals(new TableHelper().getTableImeiColumns(table),"imei");
    }

    @Test
    public void getTableSliceColumns() {
        System.out.println(new TableHelper().getTableSliceColumns(table,
                "20200221",
                "20200223"));
    }

    @Test
    public void generalHiveMeta() {
    }

    @Test
    public void firstBuildTag() {
    }

    @Test
    public void sinkName() {
    }
}