package com.oppo.tagbase.query.operator;

/**
 * Created by huangfeng on 2020/2/15.
 */
public class PersistResultOperator implements Operator {
    OperatorBuffer<AggregateRow> inputBuffer;
    OperatorBuffer<AggregateRow> outputBuffer;

    public PersistResultOperator(OperatorBuffer<AggregateRow> inputBuffer) {
        this.inputBuffer = inputBuffer;

    }

    @Override
    public OperatorBuffer getOutputBuffer() {
        return outputBuffer;
    }

    @Override
    public void run() {


        AggregateRow row;
        while((row = inputBuffer.next()) != null){
            // write HDFS
        }

        outputBuffer.offer(Row.EOF);
    }
}
