package com.oppo.tagbase.storage.core.obj;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Created by liangjingya on 2020/2/19.
 */
public class AggregateRow {

    public static final AggregateRow EOF = new AggregateRow();

    byte[][] dimValues;

    ImmutableRoaringBitmap metric;

    public AggregateRow(byte[][] dimValues, ImmutableRoaringBitmap metric) {
        this.dimValues = dimValues;
        this.metric = metric;
    }

    public AggregateRow() {
    }

    public byte[][] getDimension() {
        return dimValues;
    }

    public void setDimension(byte[][] dimension) {
        this.dimValues = dimension;
    }

    public ImmutableRoaringBitmap getMetric() {
        return metric;
    }

    public void setMetric(ImmutableRoaringBitmap metric) {
        this.metric = metric;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if(dimValues == null){
            builder.append("[ ]");
        }else {
            builder.append(Arrays.stream(dimValues).map(str-> new String(str)).collect(Collectors.toList()));
        }
        return "AggregateRow{" +
                "key=" + builder.toString() +
                " metric=" + metric +
                '}';
    }

}
