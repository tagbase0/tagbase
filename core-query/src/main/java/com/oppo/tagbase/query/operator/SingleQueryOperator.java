package com.oppo.tagbase.query.operator;

import com.oppo.tagbase.query.node.Filter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class SingleQueryOperator implements Operator {
    String id;

    List<String> dim;
    String table;
    Filter filter;

    OperatorBuffer outputBuffer;

    boolean needGroupby;


    SingleQueryOperator(String table, Filter filter, List<String> dim, OperatorBuffer outputBuffer) {
        this.table = table;
        this.filter = filter;
        this.dim = dim;
        this.outputBuffer = outputBuffer;
    }


    // 如果是标签表，dim没有定义， filter条件只有一个 直接输出
    //如果是标签表，dim没有定义，filter条件有多个值 需要汇总，输出一个
    //如果是标签表， dim定义了，不论如何都可以直接输出
    //dim定义的列+filter列为=所有维度列， 且filter条件值的范围都为1o


    public void run() {


        // get output from storage module according table filter dim
        OperatorBuffer<AggregateRow> source = null;
        AggregateRow row;

        if (!needGroupby) {
            while ((row = source.next()) != null) {
                outputBuffer.offer(row);
            }
            return;
        }

        //hash aggregate according dimensions of row
        Map<String, AggregateRow> map = new HashMap<>();
        while ((row = source.next()) != null) {
            if (map.containsKey(row.getDim())) {
                map.get(row.getDim().toString()).combine(row.getMetric(), com.oppo.tagbase.query.node.Operator.UNION);
            } else {
                map.put(row.getDim().toString(), row);
            }
        }

        // put result to output
        for (AggregateRow outRow : map.values()) {
            outputBuffer.offer(outRow.replaceSourceId(id));
        }

        outputBuffer.offer(Row.EOF);


    }


    @Override
    public OperatorBuffer getOuputBuffer() {
        return null;
    }
}



