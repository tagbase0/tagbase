package com.oppo.tagbase.query;
/**
 * @author huangfeng
 * @date 2020/2/9
 */
public class QueryExecution {
    Query query;




    QueryExecution(SemanticAnalyzer analyzer,PhysicalPlanner planner){

    }

    public QueryResponse execute() {
        // validate the query is ok in semantics
        //1. 对于查询的表， 指定的列需要真实存在
        // 2. 单表需要指定查询时间段
        // 3. 对于complexQuery 分析输出的长度 对于第一个childQuery只能输出一个（全限定了就是输出1一个），


        //转化为operator树


        // 优化， 目前应该就是知道能否一个一个输出，不影响后阶段的执行


        //operator的执行

        return null;
    }



    public static enum QueryState{
        NEW,
        RUNNING,
        FINISHED
    }

}
