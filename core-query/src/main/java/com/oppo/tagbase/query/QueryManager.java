package com.oppo.tagbase.query;

import com.oppo.tagbase.query.exception.QueryException;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.oppo.tagbase.query.exception.QueryErrorCode.QUERY_NOT_EXIST;

/**
 * @author huangfeng
 * @date 2020/2/9
 */
public class QueryManager {

    ConcurrentHashMap<String, QueryExecution> queries;

    QueryManager() {
        queries = new ConcurrentHashMap<>();
    }


    public void register(String id, QueryExecution execution) {
        queries.put(id, execution);
    }


    public Object getResult(String queryId) {
        return getQueryOrThrow(queryId).getOutput();
    }


    public boolean cancel(String queryId) {
        // add EOF to operator???
        QueryExecution execution = getQueryOrThrow(queryId);
        execution.cancel();
        queries.remove(queryId);
        return true;
    }

    public QueryExecution.QueryState queryState(String queryId) {
        return getQueryOrThrow(queryId).getState();
    }

    private QueryExecution getQueryOrThrow(String queryId) {
        QueryExecution execution = queries.get(queryId);
        if (execution == null) {
            throw new QueryException(QUERY_NOT_EXIST, "query does't exist");
        }
        return execution;
    }


    //just when queryExecution is in NEW state.
    public void remove(String queryId) {
        QueryExecution execution = queries.get(queryId);
        if (execution != null && execution.getState() == QueryExecution.QueryState.NEW) {
            queries.remove(queryId);
        }
    }

    public List getQueries() {
        return queries.values().stream().filter(query -> query.getState() == QueryExecution.QueryState.RUNNING).collect(Collectors.toList());
    }
}
