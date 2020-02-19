package com.oppo.tagbase.storage.core.example.testobj;


import com.google.common.collect.RangeSet;
import com.oppo.tagbase.meta.obj.*;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wujianchao on 2020/2/5.
 */
public class TestMetadata {

    public enum MetaDataType{
        TAG,EVENT,FLOW
    }

    private MetaDataType type = MetaDataType.EVENT;

    public void setType(MetaDataType type) {
        this.type = type;
    }

    public Table getTable(String dbName, String tableName) {
        //模拟meta模块获取到的数据
        List<Column> columns = new ArrayList<>();
        Table metaTable = null;
        switch (type){
            case EVENT:
                Column a=new Column();a.setName("version");a.setIndex(3);a.setType(ColumnType.DIM_COLUMN);
                columns.add(a);
                Column b=new Column();b.setName("app");b.setIndex(1);b.setType(ColumnType.DIM_COLUMN);
                columns.add(b);
                Column c=new Column();c.setName("event");c.setIndex(2);c.setType(ColumnType.DIM_COLUMN);
                columns.add(c);
                Column d=new Column();d.setName("imei");d.setIndex(-1);d.setType(ColumnType.BITMAP_COLUMN);
                columns.add(d);
                Column e=new Column();e.setName("daynum");e.setIndex(-1);e.setType(ColumnType.SLICE_COLUMN);
                columns.add(e);
                metaTable=new Table();
                metaTable.setName("event");metaTable.setType(TableType.ACTION);metaTable.setColumns(columns);
                break;
            case TAG:
                Column g=new Column();g.setName("city");g.setIndex(1);g.setType(ColumnType.DIM_COLUMN);
                columns.add(g);
                metaTable = new Table();
                metaTable.setName("city");metaTable.setType(TableType.TAG);metaTable.setColumns(columns);
                break;
            default:
                break;
        }

        return metaTable;

    }

    public List<Slice> getSlices(String dbName, String tableName, RangeSet<Date> range) {
        List<Slice> sliecs = new ArrayList<>();
        Slice a=new Slice();a.setStartTime(Date.valueOf("2020-02-10"));a.setSink("event_20200210");a.setStatus(SliceStatus.READY);a.setShardNum(1);
        sliecs.add(a);
        Slice b=new Slice();b.setStartTime(Date.valueOf("2020-02-09"));b.setSink("event_20200209");b.setStatus(SliceStatus.READY);b.setShardNum(1);
        sliecs.add(b);
        Slice c=new Slice();c.setStartTime(Date.valueOf("2020-02-11"));c.setSink("event_20200211");c.setStatus(SliceStatus.READY);c.setShardNum(1);
        sliecs.add(c);

        return sliecs;

    }

    public List<Slice> getSlices(String dbName, String tableName) {
        List<Slice> sliecs = new ArrayList<>();
        if(tableName.contains("event")){
            Slice a=new Slice();a.setStartTime(Date.valueOf("2020-02-10"));a.setSink("event_20200210");a.setStatus(SliceStatus.READY);a.setShardNum(1);
            sliecs.add(a);
            Slice b=new Slice();b.setStartTime(Date.valueOf("2020-02-09"));b.setSink("event_20200209");b.setStatus(SliceStatus.READY);b.setShardNum(1);
            sliecs.add(b);
            Slice c=new Slice();c.setStartTime(Date.valueOf("2020-02-11"));c.setSink("event_20200211");c.setStatus(SliceStatus.READY);c.setShardNum(1);
            sliecs.add(c);
        }else {
            Slice d=new Slice();d.setStartTime(Date.valueOf("2020-02-10"));d.setSink("city_20200210");d.setStatus(SliceStatus.READY);d.setShardNum(1);
            sliecs.add(d);
        }
        return sliecs;

    }


}
