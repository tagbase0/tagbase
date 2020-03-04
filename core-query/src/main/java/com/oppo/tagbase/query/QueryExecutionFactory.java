package com.oppo.tagbase.query;

import com.oppo.tagbase.query.node.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * @author huangfeng
 * @date 2020/2/9
 */
public class QueryExecutionFactory {
    private static Logger LOG = LoggerFactory.getLogger(QueryResource.class);
    private QueryManager queryManager;
    private SemanticAnalyzer analyzer;
    private PhysicalPlanner planner;


    @Inject
    public QueryExecutionFactory(QueryManager queryManager, SemanticAnalyzer analyzer, PhysicalPlanner planner) {
        this.queryManager = queryManager;
        this.analyzer = analyzer;
        this.planner = planner;
    }

    public QueryExecution create(String id, Query query) {
        query.setId(id);
        QueryExecution execution = new QueryExecution(query, analyzer, planner);
        queryManager.register(id, execution);
        return execution;
    }
}
