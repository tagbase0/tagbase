package com.oppo.tagbase.query.operator;


import static com.oppo.tagbase.query.operator.Row.EOF;


/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class ColletionOperator implements Operator {

    String id;

    private OperatorBuffer<AggregateRow> leftSource;
    private OperatorBuffer<AggregateRow> rightSource;
    private OperatorBuffer outputBuffer;
    private com.oppo.tagbase.query.node.Operator operator;

    public void run() {
        //index 0 buffer
        AggregateRow b = leftSource.next();

        AggregateRow c;

        while ((c = rightSource.next()) != null) {
            c.combine(b, operator);
        }


        outputBuffer.offer(EOF);
    }

    private String joinKey(String key, String key1) {
        return null;
    }


    @Override
    public OperatorBuffer getOuputBuffer() {
        return null;
    }
}
