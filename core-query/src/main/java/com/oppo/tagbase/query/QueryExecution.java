package com.oppo.tagbase.query;

import com.google.inject.Inject;
import com.oppo.tagbase.query.exception.QueryException;
import com.oppo.tagbase.query.node.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.oppo.tagbase.query.QueryExecution.QueryState.*;
import static com.oppo.tagbase.query.exception.QueryErrorCode.*;

/**
 * @author huangfeng
 * @date 2020/2/9
 * lock of QueryExecution to safeguard  both state and waitResult
 */
public class QueryExecution {
    private static Logger LOG = LoggerFactory.getLogger(QueryExecution.class);

    @Inject
    private QueryEngine queryExecutor;

    private Query query;
    private SemanticAnalyzer analyzer;
    private PhysicalPlanner planner;


    private PhysicalPlan physicalPlan;

    private Exception exception;

    private QueryState state;

    private Object result;

    QueryExecution(Query query, SemanticAnalyzer analyzer, PhysicalPlanner planner) {
        state = NEW;
        this.query = query;
        this.analyzer = analyzer;
        this.planner = planner;
    }

    public void execute() {

        Analysis analysis = analyzer.analyze(query);

        physicalPlan = planner.plan(query, analysis);

        convertState(RUNNING);

        physicalPlan.ifFinish(() -> convertState(FINISHED));

        physicalPlan.ifException((Exception e) -> {
                    exception = e;
                    convertState(Fail);
                    });

        queryExecutor.execute(physicalPlan);
    }


    public Object getOutput() {
        if (state == CANCELLING) {
            throw new QueryException(QUERY_CANCELLED, "query has been cancelled");
        }
        if (state == NEW) {
            throw new QueryException(QUERY_NOT_COMPLETE, "query has not been finished");
        }
        if (state == Fail) {
            throw new QueryException(QUERY_RUNNING_ERROR, exception);
        }
        return getResult();
    }

    private Object getResult() {
        if (result == null) {
            result = physicalPlan.getResult();
        }
        return result;
    }


    public void cancel() {
        boolean needNotifyOperator = true;
        synchronized (this) {
            if (state == CANCELLING) {
                return;
            } else if (state != RUNNING) {
                needNotifyOperator = false;
            }
            convertState(CANCELLING);
        }
        if (needNotifyOperator) {
            physicalPlan.cancel();
        }
    }


    private synchronized void convertState(QueryState newState) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("query state change, from {} to {} ", state, newState);
        }
        state = newState;
    }


    public QueryState getState() {
        return state;
    }


    public enum QueryState {
        NEW,
        RUNNING,
        FINISHED,
        CANCELLING,
        Fail
    }

}
