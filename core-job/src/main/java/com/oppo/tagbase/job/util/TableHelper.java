package com.oppo.tagbase.job.util;

import com.oppo.tagbase.job.obj.HiveDictTable;
import com.oppo.tagbase.job.obj.HiveMeta;
import com.oppo.tagbase.job.obj.HiveSrcTable;
import com.oppo.tagbase.job.obj.SliceColumn;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.MetadataDict;
import com.oppo.tagbase.meta.obj.Column;
import com.oppo.tagbase.meta.obj.ColumnType;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.JobType;
import com.oppo.tagbase.meta.obj.Slice;
import com.oppo.tagbase.meta.obj.Table;
import com.oppo.tagbase.meta.obj.TableType;
import com.oppo.tagbase.meta.obj.Task;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by daikai on 2020/2/22.
 */
public class TableHelper {

    private static String ROW_COUNT_HDFS_PATH_PRE = "/user/tagbase/data/rowcount/";

    public List<String> getTableDimColumns(Table table) {
        List<String> list = new ArrayList<>();
        TreeMap<Integer, String> map = new TreeMap<>();
        for (Column column : table.getColumns()) {
            if (ColumnType.DIM_COLUMN == column.getType()) {
                map.put(column.getIndex(), column.getSrcName());
            }
        }
        for (Map.Entry<Integer, String> entry : map.entrySet()) {
            list.add(entry.getValue());
        }
        return list;
    }

    public String getTableImeiColumns(Table table) {
        String columnImei = "";
        for (Column column : table.getColumns()) {
            if (ColumnType.BITMAP_COLUMN == column.getType()) {
                columnImei = column.getSrcName();
            }
        }
        return columnImei;
    }

    public SliceColumn getTableSliceColumns(Table table, String sliceValueLeft, String sliceValueRight) {
        String sliceName = "";
        for (Column column : table.getColumns()) {
            if (ColumnType.SLICE_COLUMN == column.getType()) {
                sliceName = column.getSrcName();
            }
        }
        return new SliceColumn(sliceName, sliceValueLeft, sliceValueRight);
    }

    public HiveMeta generalHiveMeta(Task task, Job jobRunning) {

        HiveDictTable hiveDictTable = new HiveDictTable();

        //TODO 在哪里记录反向字典的Hive表数据信息, 配置文件 ?
        hiveDictTable.setDbName("");
        hiveDictTable.setTableName("");
        hiveDictTable.setImeiColumnName("");
        hiveDictTable.setIdColumnName("");
        hiveDictTable.setSliceColumnName("");
        hiveDictTable.setMaxId((int) (new MetadataDict().getDictElementCount()));

        HiveSrcTable hiveSrcTable = new HiveSrcTable();
        hiveSrcTable.setDbName(jobRunning.getDbName());
        hiveSrcTable.setTableName(jobRunning.getTableName());
        Table table = new Metadata().getTable(jobRunning.getDbName(), jobRunning.getTableName());
        List<String> dimColumns = new TableHelper().getTableDimColumns(table);
        hiveSrcTable.setDimColumns(dimColumns);
        hiveSrcTable.setImeiColumnName(new TableHelper().getTableImeiColumns(table));
        hiveSrcTable.setSliceColumn(new TableHelper().getTableSliceColumns(table,
                jobRunning.getDataLowerTime().toString().replace("-", ""),
                jobRunning.getDataUpperTime().toString().replace("-", "")));


        String output = task.getOutput();
        if (JobType.DICTIONARY == jobRunning.getType()) {

            output = new Date(System.currentTimeMillis()).toString();
        }

        //TODO set rowCountPath
        String rowCountPath = ROW_COUNT_HDFS_PATH_PRE + task.getId();

        return new HiveMeta(hiveDictTable, hiveSrcTable, output, rowCountPath);
    }

    public boolean firstBuildTag(String dbName, String tableName) {
        Table table = new Metadata().getTable(dbName, tableName);
        if (table == null || table.getLatestSlice() == null) {
            return true;
        }

        List<Slice> sliceList = new Metadata().getSlices(dbName, tableName);
        if (sliceList == null || sliceList.size() == 0) {
            return true;
        }

        String sink = sliceList.get(sliceList.size() - 1).getSink();
        if (sink == null || sink.length() == 0) {
            return true;
        }

        return false;
    }

    public String sinkName(String dbName, String tableName) {
        Table table = new Metadata().getTable(dbName, tableName);
        if (TableType.TAG == table.getType()) {
            // like city, gender
            return table.getName();
        } else {
            // like dayno_app_from
            List<String> dimsColumsName = new TableHelper().getTableDimColumns(table);
            StringBuffer buffer = new StringBuffer();
            for (String s : dimsColumsName) {
                buffer.append(s);
            }
            return buffer.toString();
        }
    }
}
