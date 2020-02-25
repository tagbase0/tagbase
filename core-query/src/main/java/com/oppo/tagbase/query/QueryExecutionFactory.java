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
    private IdGenerator idGenerator;
    private SemanticAnalyzer analyzer;
    private PhysicalPlanner planner;


    @Inject
    public QueryExecutionFactory(QueryManager queryManager, IdGenerator idGenerator) {
        this.queryManager = queryManager;
        this.idGenerator = idGenerator;
    }

    public QueryExecution create(String id, Query query) {

        QueryExecution execution = new QueryExecution(query, analyzer, planner);
        queryManager.register(id, execution);
        return execution;
    }
}
