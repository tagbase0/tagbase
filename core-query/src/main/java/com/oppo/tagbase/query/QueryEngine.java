package com.oppo.tagbase.query;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.oppo.tagbase.query.operator.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * @author huangfeng
 * @date 2020/2/11 15:48
 */
public class QueryEngine {
    private static Logger LOG = LoggerFactory.getLogger(QueryEngine.class);
    ExecutorService service;

    @Inject
    public QueryEngine() {
    }

    public QueryEngine(ExecutorService service) {
        this.service = service;
    }


    public void execute(PhysicalPlan physicalPlan) {
        for(Operator operator:physicalPlan.getOperators()){
            service.execute(operator);
        }
    }


    @VisibleForTesting
    public void destory() {
        service.shutdownNow();
    }

}
