package com.oppo.tagbase.query.operator;

import com.oppo.tagbase.query.node.OperatorType;
import com.oppo.tagbase.storage.core.connector.StorageConnector;
import com.oppo.tagbase.storage.core.obj.QueryHandler;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.Map;

/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class SingleQueryOperator implements Operator {
    String sourceId;

    OperatorBuffer outputBuffer;
    QueryHandler queryHandler;
    int groupMaxsize;

    StorageConnector connector;

    public SingleQueryOperator(QueryHandler queryHandler, OperatorBuffer outputBuffer, StorageConnector connector,int groupMaxSize,String sourceId) {
        this.outputBuffer = outputBuffer;
        this.queryHandler = queryHandler;
        this.connector = connector;
        this.groupMaxsize = groupMaxSize;
        this.sourceId = sourceId;
    }


    public void run() {

        // get output from storage module according table filter dim
//        OperatorBuffer<AggregateRow> source = connector.createQuery(queryHandler);
        OperatorBuffer<AggregateRow> source = null;

        AggregateRow row;

        //hash aggregate according dimensions of row
        Map<String, Pair<AggregateRow, Integer>> map = new HashMap<>();

        while ((row = source.next()) != null) {
            row.setId(sourceId);

            if (map.containsKey(row.getDim())) {

                Pair<AggregateRow, Integer> pair = map.get(row.getDim().getSignature());
                AggregateRow groupRow = pair.getValue0();
                int groupCount = pair.getValue1();

                groupRow.combine(row.getMetric(), OperatorType.UNION);
                groupCount++;
                if (groupCount == groupMaxsize) {
                    outputBuffer.offer(groupRow);
                    map.remove(row.getDim().getSignature());
                } else {
                    map.put(row.getDim().getSignature(), new Pair<>(groupRow, groupCount));
                }

            } else {
                map.put(row.getDim().toString(), new Pair<>(row, 1));
            }
        }

        // put result to output
        map.values().forEach(pair -> outputBuffer.offer(pair.getValue0()));
        outputBuffer.offer(Row.EOF);


    }


    @Override
    public OperatorBuffer getOutputBuffer() {
        return outputBuffer;
    }
}



