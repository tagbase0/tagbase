package com.oppo.tagbase.storage.core.obj;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

/**
 * Created by liangjingya on 2020/2/19.
 */
public class RawRow extends AbstractRow {

   protected ImmutableRoaringBitmap metric;

    public RawRow(byte[][] dimValues, ImmutableRoaringBitmap metric) {
        super(new Dimensions(dimValues));
        this.metric = metric;
    }
    public RawRow(Dimensions dimensions, ImmutableRoaringBitmap metric) {
        super(dimensions);
        this.metric = metric;
    }

    public ImmutableRoaringBitmap getMetric() {
        return metric;
    }

    @Override
    public String toString() {
        return "AggregateRow{" +
                "key=" + dims.toString() +
                " metric=" + metric +
                '}';
    }


}
