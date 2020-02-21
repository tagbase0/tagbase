package com.oppo.tagbase.query.operator;


import com.oppo.tagbase.query.node.OperatorType;
import com.oppo.tagbase.query.node.OutputType;

import java.util.ArrayList;
import java.util.List;

import static com.oppo.tagbase.query.operator.Row.EOF;


/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class CollectionOperator implements Operator {


    private OperatorBuffer<AggregateRow> leftSource;
    private OperatorBuffer<AggregateRow> rightSource;
    private OperatorBuffer outputBuffer;
    private OperatorType operator;
    private OutputType outputType;


    public CollectionOperator(OperatorBuffer<AggregateRow> leftSource, OperatorBuffer<AggregateRow> rightSource, OperatorBuffer outputBuffer, OperatorType operator, OutputType outputType) {
        this.leftSource = leftSource;
        this.rightSource = rightSource;
        this.outputBuffer = outputBuffer;
        this.outputType = outputType;
        this.operator = operator;
    }

    public void run() {

        //index 0 buffer
        List<AggregateRow> buildRows = new ArrayList<>();
        AggregateRow row;
        while ((row = leftSource.next()) != null) {
            buildRows.add(row);
        }


        AggregateRow c;
        if (outputType == OutputType.COUNT) {

            while ((c = rightSource.next()) != null) {

                for (int n = 0; n < buildRows.size() - 1; n++) {
                    outputBuffer.offer(AggregateRow.combineAndTransitToResult(buildRows.get(n), c, operator));
                }

                c.combineAndTransitToResult(buildRows.get(buildRows.size() - 1), operator);
                outputBuffer.offer(c);

            }


        } else {

            while ((c = rightSource.next()) != null) {

                for (int n = 0; n < buildRows.size() - 1; n++) {
                    outputBuffer.offer(AggregateRow.combine(buildRows.get(n), c, operator));
                }
                c.combine(buildRows.get(buildRows.size() - 1), operator);
                outputBuffer.offer(c);

            }
        }

        outputBuffer.offer(EOF);
    }



    @Override
    public OperatorBuffer getOutputBuffer() {
        return outputBuffer;
    }
}
