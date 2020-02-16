package com.oppo.tagbase.query.operator;

import com.oppo.tagbase.query.node.Operator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.nio.ByteBuffer;

/**
 * Created by huangfeng on 2020/2/14.
 */
public class AggregateRow extends AbstractRow {

    int metaId;
    ImmutableRoaringBitmap metric;

    public AggregateRow(byte[][] dims,byte[]  bitmap){

        ByteBuffer byteBuffer = ByteBuffer.wrap(bitmap);
        this.metric = new ImmutableRoaringBitmap(byteBuffer);
        this.dims = new Dimensions(dims);
    }


    public ImmutableRoaringBitmap getMetric() {
        return metric;
    }

    public void combine(ImmutableRoaringBitmap bitmap, Operator operator) {

        if (metric instanceof MutableRoaringBitmap) {
            switch (operator) {
                case INTERSECT:
                    ((MutableRoaringBitmap) metric).and(bitmap);
                    break;
                case UNION:
                    ((MutableRoaringBitmap) metric).or(bitmap);
                    break;
                case DIFF:
                    ((MutableRoaringBitmap) metric).andNot(bitmap);
                    break;
                default:break;
            }
        } else {
            switch (operator) {
                case INTERSECT:
                    metric = ImmutableRoaringBitmap.and(bitmap, metric);
                    break;
                case UNION:
                    metric = ImmutableRoaringBitmap.or(bitmap, metric);
                    break;
                case DIFF:
                    metric = ImmutableRoaringBitmap.andNot(bitmap, metric);
                    break;
                default:
                    break;
            }
        }


    }

    public void combine(AggregateRow row, Operator operator) {







    }

    public int combineAndOutputCardinality(Row b, Operator operator) {
        return 0;
    }

    public boolean matchDim(Dimensions dimensions) {
        return dims.equals(dimensions);
    }


    public ResultRow transitToResult() {
        return new ResultRow(dims, metric.getCardinality());
    }

    public AggregateRow replaceSourceId(String id) {
        return this;
    }
}
