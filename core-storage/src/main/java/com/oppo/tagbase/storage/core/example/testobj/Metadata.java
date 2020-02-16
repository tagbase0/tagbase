package com.oppo.tagbase.storage.core.example.testobj;


import java.util.ArrayList;
import java.util.List;

/**
 * Created by wujianchao on 2020/2/5.
 */
public class Metadata {

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
                columns.add(new Column("version",3, ColumnType.DIM_COLUMN));
                columns.add(new Column("app",1, ColumnType.DIM_COLUMN));
                columns.add(new Column("event",2, ColumnType.DIM_COLUMN));
                columns.add(new Column("imei",-1, ColumnType.BITMAP_COLUMN));
                columns.add(new Column("daynum",-1, ColumnType.SLICE_COLUMN));
                metaTable = new Table("event", TableType.ACTION, columns);
                break;
            case TAG:
                columns.add(new Column("city",1, ColumnType.DIM_COLUMN));
                metaTable = new Table("city", TableType.TAG, columns);
                break;
            default:
                break;
        }

        return metaTable;

    }

    public List<Slice> getSlices(String dbName, String tableName) {
        List<Slice> sliecs = new ArrayList<>();
        switch (type){
            case EVENT:
                sliecs.add(new Slice("20200210", "event_20200210", SliceStatus.READY));
                sliecs.add(new Slice("20200209", "event_20200209", SliceStatus.READY));
                sliecs.add(new Slice("20200211", "event_20200211", SliceStatus.READY));
                break;
            case TAG:
                sliecs.add(new Slice("", "city_20200210", SliceStatus.READY));
                sliecs.add(new Slice("", "city_20200209", SliceStatus.DISABLED));
                break;
            default:
                break;
        }

        return sliecs;

    }



}
