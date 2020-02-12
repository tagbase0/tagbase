package com.oppo.tagbase.query;

import com.oppo.tagbase.query.node.Query;
import com.oppo.tagbase.query.operator.Operator;
import com.oppo.tagbase.query.operator.OperatorBuffer;

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

    QueryExecution(SemanticAnalyzer analyzer,PhysicalPlanner planner){
        this.analyzer = analyzer;
        this.planner = planner;
    }

    public QueryResponse execute() {

        //semantic analyze
        analyzer.analyze(query);
        //转化为operator树



        List<Operator> physicalPlan = planner.plan(query);
        // 优化， 目前应该就是知道能否一个一个输出，不影响后阶段的执行

        for(Operator operator: physicalPlan){
            queryExecutor.execute(operator);
        }
        

        //operator的执行

        return wrapResult(physicalPlan.get(physicalPlan.size()-1).getOuputBuffer());
    }

    private QueryResponse wrapResult(OperatorBuffer ouputBuffer) {
        return null;
    }


    public  enum QueryState{
        NEW,
        RUNNING,
        FINISHED
    }

}
