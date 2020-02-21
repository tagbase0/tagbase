package com.oppo.tagbase.query;

import com.google.common.collect.ImmutableMap;
import com.oppo.tagbase.query.operator.Operator;
import com.oppo.tagbase.query.operator.OperatorBuffer;
import com.oppo.tagbase.query.operator.ResultRow;
import com.oppo.tagbase.query.operator.RowMeta;

import java.util.Map;

/**
 * @author huangfeng
 * @date 2020/2/18 19:15
 */
public class OutputOperator implements Operator {

    Map<String, RowMeta> outputMeta;
    OperatorBuffer<Map<String, Object>> outputBuffer;
    OperatorBuffer<ResultRow> inputBuffer;

    public OutputOperator(OperatorBuffer<ResultRow> inputBuffer, Map<String, RowMeta> outputMeta) {
        this.inputBuffer = inputBuffer;
        outputBuffer = new OperatorBuffer<>();
        this.outputMeta = outputMeta;
    }

    @Override
    public OperatorBuffer getOutputBuffer() {
        return outputBuffer;
    }

    @Override
    public void run() {
        ResultRow row;
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

            outputBuffer.offer(mapRowBuilder.build());
        }


    }
}
