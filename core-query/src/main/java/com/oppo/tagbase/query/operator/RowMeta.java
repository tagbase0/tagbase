package com.oppo.tagbase.query.operator;

import com.google.common.collect.ImmutableList;
import com.oppo.tagbase.meta.type.DataType;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by huangfeng on 2020/2/15.
 */
public class RowMeta {

    ConcurrentHashMap<String,RowMeta> cache;

    private List<String> columns;
    private  List<DataType> fields;
    int id;

    public RowMeta(ImmutableList<String> stringBuilder, int i) {
    }




    public  RowMeta combine(RowMeta other){
        if(cache.contains(id+""+other.id)){
            return cache.get(id+""+other.id);
        }

        int newID = 1;
         return new RowMeta(ImmutableList.<String>builder().addAll(other.columns).addAll(columns).build(),1);

    }



}
