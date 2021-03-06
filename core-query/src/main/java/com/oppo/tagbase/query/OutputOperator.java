package com.oppo.tagbase.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.oppo.tagbase.query.operator.AbstractOperator;
import com.oppo.tagbase.query.row.ResultRow;
import com.oppo.tagbase.query.row.RowMeta;
import com.oppo.tagbase.storage.core.obj.OperatorBuffer;

import java.util.List;
import java.util.Map;

/**
 * @author huangfeng
 * @date 2020/2/18 19:15
 */
public class OutputOperator extends AbstractOperator {

    Map<String, RowMeta> outputMeta;
    //    OperatorBuffer<Map<String, Object>> outputBuffer;
    OperatorBuffer<ResultRow> inputBuffer;

    public OutputOperator(int id, OperatorBuffer<ResultRow> inputBuffer, Map<String, RowMeta> outputMeta) {
        super(id, new OperatorBuffer<List<Map<String, Object>>>());
        this.inputBuffer = inputBuffer;
        this.outputMeta = outputMeta;
    }


    @Override
    public void internalRun() {
        ResultRow row;
        ImmutableList.Builder<Map<String, Object>> resultBuilder = ImmutableList.builder();
        while ((row = inputBuffer.next()) != null) {
            ImmutableMap.Builder mapRowBuilder = ImmutableMap.<String, Object>builder();

            RowMeta rowMeta = outputMeta.get(row.id());


            for (int n = 0; n < row.getDim().length(); n++) {
                String columnName = rowMeta.getColumnName(n);
                Object dim;

                //dim just string type now
                dim = row.getDim().getString(n);

                mapRowBuilder.put(columnName, dim);
            }

            mapRowBuilder.put("metric", row.getMetric());
            resultBuilder.add(mapRowBuilder.build());
        }
        outputBuffer.postData(resultBuilder.build());
        outputBuffer.postEnd();
    }

    @Override
    public String toString() {
        return "OutputOperator{" +
                outputMeta.values()+
                '}';
    }
}
