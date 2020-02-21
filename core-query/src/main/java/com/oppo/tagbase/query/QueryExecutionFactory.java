package com.oppo.tagbase.query;

import com.oppo.tagbase.query.node.Query;

import javax.inject.Inject;

/**
 * @author huangfeng
 * @date 2020/2/9
 */
public class QueryExecutionFactory {
    QueryManager queryManager;
    IdGenerator idGenerator;
    SemanticAnalyzer analyzer;
    PhysicalPlanner planner;


    @Inject
    public QueryExecutionFactory(QueryManager queryManager, IdGenerator idGenerator) {
        this.queryManager = queryManager;
        this.idGenerator = idGenerator;
    }

    public QueryExecution create(String id,Query query) {
        QueryExecution execution = new QueryExecution(query, analyzer, planner);
        queryManager.register(id, execution);
        return execution;
    }
}
