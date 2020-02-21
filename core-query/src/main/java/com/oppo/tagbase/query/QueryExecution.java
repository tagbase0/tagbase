package com.oppo.tagbase.query;

import com.oppo.tagbase.query.node.Query;
import com.oppo.tagbase.query.operator.Operator;
import com.oppo.tagbase.query.operator.OperatorBuffer;
import com.oppo.tagbase.query.operator.ResultRow;

import java.util.List;

/**
 * @author huangfeng
 * @date 2020/2/9
 */
public class QueryExecution {
    Query query;

    SemanticAnalyzer analyzer;
    PhysicalPlanner planner;
    QueryEngine queryExecutor;
    List<Operator> physicalPlan;
    QueryState state;


    QueryExecution(Query query, SemanticAnalyzer analyzer, PhysicalPlanner planner) {
        this.query = query;
        this.analyzer = analyzer;
        this.planner = planner;
    }

    public void execute() {

        //semantic analyze
       Analysis analysis =  analyzer.analyze(query);

        //转化为operator树
        physicalPlan = planner.plan(query,analysis);

        for (Operator operator : physicalPlan) {
            queryExecutor.execute(operator);
        }
    }

    public QueryResponse getOutput() {
        return wrapResult(physicalPlan.get(physicalPlan.size() - 1).getOutputBuffer());
    }

    private QueryResponse wrapResult(OperatorBuffer<ResultRow> outputBuffer) {


        return null;
    }

    public QueryState getState() {
        return state;
    }

    public enum QueryState {
        NEW,
        RUNNING,
        FINISHED
    }

}
