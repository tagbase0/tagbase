package com.oppo.tagbase.query.operator;

import com.oppo.tagbase.query.node.ComplexQuery;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * Created by huangfeng on 2020/2/14.
 */
public class AggregateRow extends AbstractRow {

    ImmutableRoaringBitmap metric;

    public ImmutableRoaringBitmap getMetric() {
        return metric;
    }


    public void combine(ImmutableRoaringBitmap bitmap, ComplexQuery.Operator operator) {

        if (metric instanceof MutableRoaringBitmap) {
            switch (operator) {
                case intersect:
                    ((MutableRoaringBitmap) metric).and(bitmap);
                    break;
                case union:
                    ((MutableRoaringBitmap) metric).or(bitmap);
                    break;
                case diff:
                    ((MutableRoaringBitmap) metric).andNot(bitmap);
                    break;
                default:break;
            }
        } else {
            switch (operator) {
                case intersect:
                    metric = ImmutableRoaringBitmap.and(bitmap, metric);
                    break;
                case union:
                    metric = ImmutableRoaringBitmap.or(bitmap, metric);
                    break;
                case diff:
                    metric = ImmutableRoaringBitmap.andNot(bitmap, metric);
                    break;
                default:
                    break;
            }
        }


    }

    public void combine(Row row, ComplexQuery.Operator operator) {
    }

    public int combineAndOutputCardinality(Row b, ComplexQuery.Operator operator) {
        return 0;
    }

    public boolean matchDim(Dimensions dimensions) {
        return dims.equals(dimensions);
    }


    public ResultRow transitToResult() {
        return new ResultRow(dims, metric.getCardinality());
    }

    public AggregateRow replaceSourceId(String id) {
        this.sourceId = id;
        return this;
    }
}
