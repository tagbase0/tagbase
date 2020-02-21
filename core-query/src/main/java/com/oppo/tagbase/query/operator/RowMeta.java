package com.oppo.tagbase.query.operator;

import com.google.common.collect.ImmutableList;
import com.oppo.tagbase.meta.type.DataType;

import java.util.List;

/**
 * Created by huangfeng on 2020/2/15.
 */
public class RowMeta {


    private List<String> columns;
    private List<DataType> fields;
    String id;

    public RowMeta(List<String> columns, List<DataType> fields, String id) {
        this.columns = columns;
        this.fields = fields;
        this.id = id;
    }

    public RowMeta(List<String> columns, List<DataType> fields) {
        this.columns = columns;
        this.fields = fields;
    }

    public static RowMeta join(RowMeta leftRowMeta, RowMeta rightrowMeta) {
        String newID = combineID(leftRowMeta.getID(), rightrowMeta.getID());
        List<String> newColumns = ImmutableList.<String>builder().addAll(leftRowMeta.columns).addAll(rightrowMeta.columns).build();
        List<DataType> newFields = ImmutableList.<DataType>builder().addAll(leftRowMeta.fields).addAll(rightrowMeta.fields).build();

        return new RowMeta(newColumns, newFields, newID);
    }

    private static String combineID(String id1, String id2) {
        return id1 + "," + id2;
    }





    public void set(String outputID) {
        this.id = outputID;
    }

    public String getID() {
        return id;
    }

    public String getColumnName(int n) {
        return columns.get(n);
    }

    public DataType getType(int n) {
        return fields.get(n);
    }
}
