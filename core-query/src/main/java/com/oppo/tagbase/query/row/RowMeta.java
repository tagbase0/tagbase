package com.oppo.tagbase.query.row;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.oppo.tagbase.meta.type.DataType;

import java.util.List;

/**
 * Created by huangfeng on 2020/2/15.
 */
public class RowMeta {


    private String id;
    private List<String> columns;
    private List<DataType> fields;

    public RowMeta(String id,List<String> columns, List<DataType> fields) {
        Preconditions.checkArgument(columns.size() == fields.size());
        this.columns = columns;
        this.fields = fields;
        this.id = id;
    }


    public static RowMeta join(RowMeta leftRowMeta, RowMeta rightRowMeta) {
        String newID = combineID(leftRowMeta.getID(), rightRowMeta.getID());
        List<String> newColumns = ImmutableList.<String>builder().addAll(leftRowMeta.columns).addAll(rightRowMeta.columns).build();
        List<DataType> newFields = ImmutableList.<DataType>builder().addAll(leftRowMeta.fields).addAll(rightRowMeta.fields).build();

        return new RowMeta(newID,newColumns, newFields);
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
