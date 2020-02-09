package com.oppo.tagbase.query;

import javax.inject.Inject;
/**
 * @author huangfeng
 * @date 2020/2/9
 */
public class QueryExecutionFactory {
    QueryManager queryManager;

    @Inject
    public QueryExecutionFactory(QueryManager queryManager){
        this.queryManager = queryManager;
    }

    public  QueryExecution create(Query query) {
        QueryExecution execution = new QueryExecution(null,null);
        queryManager.register(execution);
        return execution;
    }
}
