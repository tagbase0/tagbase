package com.oppo.tagbase.query;

import com.oppo.tagbase.query.node.Query;

/**
 * @author huangfeng
 * @date 2020/2/9
 */
public class QueryExecution {
    Query query;

    SemanticAnalyzer analyzer;
    PhysicalPlanner planner;

    QueryExecution(SemanticAnalyzer analyzer,PhysicalPlanner planner){
        this.analyzer = analyzer;
        this.planner = planner;
    }

    public QueryResponse execute() {

        //semantic analyze
        analyzer.analyze(query);
        //转化为operator树




        // 优化， 目前应该就是知道能否一个一个输出，不影响后阶段的执行


        //operator的执行

        return null;
    }



    public  enum QueryState{
        NEW,
        RUNNING,
        FINISHED
    }

}
