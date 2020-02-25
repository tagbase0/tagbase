package com.oppo.tagbase.query;

import com.google.inject.Inject;
import com.oppo.tagbase.query.exception.QueryException;
import com.oppo.tagbase.query.node.Query;
import com.oppo.tagbase.query.operator.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.oppo.tagbase.query.QueryExecution.QueryState.*;
import static com.oppo.tagbase.query.exception.QueryErrorCode.*;

/**
 * @author huangfeng
 * @date 2020/2/9
 * lock of QueryExecution to safeguard  both state and waitResult
 */
public class QueryExecution {
    private static Logger LOG = LoggerFactory.getLogger(QueryResource.class);

    private Query query;

    private SemanticAnalyzer analyzer;
    private PhysicalPlanner planner;

    @Inject
    private QueryEngine queryExecutor;

    private List<Operator> physicalPlan;

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

        //semantic analyze
        Analysis analysis = analyzer.analyze(query);

        //转化为operator树
        physicalPlan = planner.plan(query, analysis);

        convertState(RUNNING);

        physicalPlan.get(physicalPlan.size() - 1).ifFinish(() -> convertState(FINISHED));

        for (Operator operator : physicalPlan) {
            operator.ifException((Exception e) -> {
                exception = e;
                convertState(Fail);
                //cancel
            });
        }


        for (Operator operator : physicalPlan) {
            queryExecutor.execute(operator);
        }

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
            result = physicalPlan.get(physicalPlan.size() - 1).getOutputBuffer().next();
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
            for (Operator operator : physicalPlan) {
                operator.cancelOutput();
            }
        }
    }


    private synchronized void convertState(QueryState newState) {
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
