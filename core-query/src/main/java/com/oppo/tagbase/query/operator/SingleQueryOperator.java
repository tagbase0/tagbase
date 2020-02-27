package com.oppo.tagbase.query.operator;

import com.oppo.tagbase.query.node.OperatorType;
import com.oppo.tagbase.query.node.OutputType;
import com.oppo.tagbase.query.row.AggregateRow;
import com.oppo.tagbase.storage.core.connector.StorageConnector;
import com.oppo.tagbase.storage.core.obj.OperatorBuffer;
import com.oppo.tagbase.storage.core.obj.QueryHandler;
import com.oppo.tagbase.storage.core.obj.RawRow;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.Map;

/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class SingleQueryOperator extends AbstractOperator {

    private StorageConnector connector;
    private QueryHandler queryHandler;

    private String sourceId;
    private int groupMaxsize;
    private OutputType outputType;

    public SingleQueryOperator(int operatorId, QueryHandler queryHandler, OperatorBuffer outputBuffer,OutputType outPutType, StorageConnector connector, int groupMaxSize, String sourceId) {
        super(operatorId,outputBuffer);
        this.queryHandler = queryHandler;
        this.outputType = outPutType;
        this.connector = connector;
        this.groupMaxsize = groupMaxSize;
        this.sourceId = sourceId;
    }



    @Override
    public void internalRun() {

        // get output from storage module according table filter dim
//        OperatorBuffer<AggregateRow> source = connector.createQuery(queryHandler);
        OperatorBuffer<RawRow> source = connector.createQuery(queryHandler);

        RawRow rawRow;

        //hash aggregate according dimensions of row
        Map<String, Pair<AggregateRow, Integer>> map = new HashMap<>();

        while ((rawRow = source.next()) != null) {
            AggregateRow row = new AggregateRow(sourceId,rawRow.getDim(),rawRow.getMetric());
//            row.setId(sourceId);

            if (map.containsKey(row.getDim().toString())) {

                Pair<AggregateRow, Integer> pair = map.get(row.getDim().toString());
                AggregateRow groupRow = pair.getValue0();
                int groupCount = pair.getValue1();

                groupRow.combine(row.getMetric(), OperatorType.UNION);
                groupCount++;
                if (groupCount == groupMaxsize) {
                    sendData(groupRow);
                    map.remove(row.getDim().toString());
                } else {
                    map.put(row.getDim().toString(), new Pair<>(groupRow, groupCount));
                }

            } else {
                map.put(row.getDim().toString(), new Pair<>(row, 1));
            }
        }

        // put result to output
        map.values().forEach(pair -> sendData(pair.getValue0()));
        outputBuffer.postEnd();
    }

    private void sendData(AggregateRow groupRow) {
        if(outputType == OutputType.COUNT) {
            outputBuffer.postData(groupRow.transitToResult());
        }else{
            outputBuffer.postData(groupRow);
        }
    }


    @Override
    public String toString() {
        return String.format("SingleQueryOperator{scanTable=%s,outputType=%s}",queryHandler.getTableName(),outputType);
    }
}



