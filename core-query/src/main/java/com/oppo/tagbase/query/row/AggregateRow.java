package com.oppo.tagbase.query.row;

import com.oppo.tagbase.query.node.OperatorType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.nio.ByteBuffer;

/**
 * Created by huangfeng on 2020/2/14.
 */
public class AggregateRow extends AbstractRow {
    ImmutableRoaringBitmap metric;

    public AggregateRow(byte[][] dims, byte[] bitmap) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bitmap);
        this.metric = new ImmutableRoaringBitmap(byteBuffer);
        this.dims = new Dimensions(dims);
    }

    public AggregateRow(String id, Dimensions dims, ImmutableRoaringBitmap metric) {
        this.metric = metric;
        this.dims = dims;
        this.id = id;
    }

    public static AggregateRow combine(AggregateRow a, AggregateRow b, OperatorType operator) {
        ImmutableRoaringBitmap metric = null;
        switch (operator) {
            case INTERSECT:
                metric = ImmutableRoaringBitmap.and(a.metric, b.metric);
                break;
            case UNION:
                metric = ImmutableRoaringBitmap.or(a.metric, b.metric);
                break;
            case DIFF:
                metric = ImmutableRoaringBitmap.andNot(a.metric, b.metric);
                break;
            default:
                break;
        }

        Dimensions newDimensions = Dimensions.join(a.getDim(), b.getDim());
        String id = joinKey(a.id, b.id);
        return new AggregateRow(id, newDimensions, metric);
    }

    public static ResultRow combineAndTransitToResult(AggregateRow a, AggregateRow b, OperatorType operator) {

        int metric = -1;
        switch (operator) {
            case INTERSECT:
                metric = ImmutableRoaringBitmap.andCardinality(a.metric, b.metric);
                break;
            case UNION:
                metric = ImmutableRoaringBitmap.orCardinality(a.metric, b.metric);
                break;
            case DIFF:
                metric = ImmutableRoaringBitmap.andNotCardinality(a.metric, b.metric);
                break;
            default:
                break;
        }
        Dimensions newDimensions = Dimensions.join(a.getDim(), b.getDim());
        String id = joinKey(a.id, b.id);
        return new ResultRow(id, newDimensions,metric);
    }


    public ImmutableRoaringBitmap getMetric() {
        return metric;
    }

    public void combine(ImmutableRoaringBitmap bitmap, OperatorType operator) {

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
                default:
                    break;
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

    public void combine(AggregateRow row, OperatorType operator) {
        combine(row.getMetric(), operator);
        id = joinKey(row.id, id);
        dims = Dimensions.join(row.getDim(),dims);
    }

    public ResultRow combineAndTransitToResult(AggregateRow b, OperatorType operator) {
        ImmutableRoaringBitmap bitmap = b.getMetric();
        int cardinality = -1;

        switch (operator) {
            case INTERSECT:
                cardinality = ImmutableRoaringBitmap.andCardinality(bitmap, metric);
                break;
            case UNION:
                cardinality = ImmutableRoaringBitmap.orCardinality(bitmap, metric);
                break;
            case DIFF:
                cardinality = ImmutableRoaringBitmap.andNotCardinality(bitmap, metric);
                break;
            default:
                break;
        }
        id = joinKey(b.id, id);
        return new ResultRow(dims, cardinality);
    }


    private static String joinKey(String id, String id1) {
        return id + id1;
    }


    public ResultRow transitToResult() {
        return new ResultRow(dims, metric.getCardinality());
    }


}
