package com.oppo.tagbase.query.operator;


import com.oppo.tagbase.query.node.ComplexQuery;
import com.oppo.tagbase.query.node.Query;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static com.oppo.tagbase.query.operator.Row.EOF;


/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class ColletionOperator implements Operator {

    String id;
    List<OperatorBuffer> buffers;

    OperatorBuffer<AggregateRow> leftSource;
    OperatorBuffer<AggregateRow> rightSource;


    OperatorBuffer outputBuffer;


    ComplexQuery.Operator operator;

    LinkedBlockingQueue output;

    Query.OutputType outputType;


    public void work() {
        //index 0 buffer


        Row b = leftSource.next();

        AggregateRow c;

        if (outputType == Query.OutputType.BITMAP) {
            while ((c = rightSource.next()) != null) {
                c.combine(b, operator);
            }

        } else if (outputType == Query.OutputType.COUNT) {
            while ((c = rightSource.next()) != null) {
                c.combineAndOutputCardinality(b, operator);
            }

        }


        output.offer(EOF);
    }

    private String joinKey(String key, String key1) {
        return null;
    }


    @Override
    public OperatorBuffer getOuputBuffer() {
        return null;
    }
}
