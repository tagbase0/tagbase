package com.oppo.tagbase.query.operator;

import com.oppo.tagbase.query.node.ComplexQuery;
import com.oppo.tagbase.query.node.Filter;
import com.oppo.tagbase.query.node.Query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.oppo.tagbase.query.node.Query.OutputType.COUNT;

/**
 * @author huangfeng
 * @date 2020/2/8
 */
public class SingleQueryOperator implements Operator {
    String id;

    List<String> dim;
    String table;
    Filter filter;

    OperatorBuffer outPutBuffer;

    ComplexQuery.Operator operator;

    boolean needGroupby;

    Query.OutputType outputType;

    SingleQueryOperator(List<String> dim) {
        // 如果是标签表，dim没有定义， filter条件只有一个 直接输出
        //如果是标签表，dim没有定义，filter条件有多个值 需要汇总，输出一个
        //如果是标签表， dim定义了，不论如何都可以直接输出
        //dim定义的列+filter列为=所有维度列， 且filter条件值的范围都为1o

    }


    public void work() {
        Map<String, AggregateRow> map = new HashMap<>();


        // get output from storage module according table filter dim
        OperatorBuffer<AggregateRow> source = null;

        AggregateRow row;


        while ((row = source.next()) != null) {
            if (map.containsKey(row.getDim())) {
                map.get(row.getDim().toString()).combine(row.getMetric(), operator);
            } else {
                map.put(row.getDim().toString(), row);
            }
        }


        if (outputType == COUNT) {

            for (AggregateRow outRow : map.values()) {
                ResultRow resultRow = outRow.transitToResult();
                outPutBuffer.offer(resultRow);
            }

        } else {
            // put result to output
            for (AggregateRow outRow : map.values()) {
                outPutBuffer.offer(outRow.replaceSourceId(id));
            }
        }

        outPutBuffer.offer(Row.EOF);


    }


    @Override
    public OperatorBuffer getOuputBuffer() {
        return null;
    }
}



