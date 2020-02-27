package com.oppo.tagbase.query.operator;


import com.oppo.tagbase.query.node.OperatorType;
import com.oppo.tagbase.query.node.OutputType;
import com.oppo.tagbase.query.row.AggregateRow;
import com.oppo.tagbase.storage.core.obj.OperatorBuffer;

import java.util.ArrayList;
import java.util.List;



/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class CollectionOperator extends  AbstractOperator {

    private OperatorBuffer<AggregateRow> rightSource;
    private OperatorBuffer<AggregateRow> leftSource;
    private OperatorType operator;
    private OutputType outputType;


    public CollectionOperator(int id, OperatorBuffer<AggregateRow> leftSource, OperatorBuffer<AggregateRow> rightSource, OperatorBuffer outputBuffer, OperatorType operator, OutputType outputType) {
        super(id,outputBuffer);
        this.leftSource = leftSource;
        this.rightSource = rightSource;
        this.outputType = outputType;
        this.operator = operator;
    }




    @Override
    public void internalRun() {
        //TODO buildRow size need limit
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
                    outputBuffer.postData(AggregateRow.combineAndTransitToResult(buildRows.get(n), c, operator));
                }

                c.combineAndTransitToResult(buildRows.get(buildRows.size() - 1), operator);
                outputBuffer.postData(c);

            }


        } else {

            while ((c = rightSource.next()) != null) {

                for (int n = 0; n < buildRows.size() - 1; n++) {
                    outputBuffer.postData(AggregateRow.combine(buildRows.get(n), c, operator));
                }
                c.combine(buildRows.get(buildRows.size() - 1), operator);
                outputBuffer.postData(c);

            }
        }

        outputBuffer.postEnd();
    }


    @Override
    public String toString() {
        return "CollectionOperator{" +
                "operator=" + operator +
                '}';
    }
}
