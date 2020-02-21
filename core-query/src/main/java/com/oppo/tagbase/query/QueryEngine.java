package com.oppo.tagbase.query;

import com.google.inject.Inject;
import com.oppo.tagbase.query.operator.Operator;

import java.util.concurrent.ExecutorService;

/**
 * @author huangfeng
 * @date 2020/2/11 15:48
 */
public class QueryEngine {

    ExecutorService service;
    @Inject
    public QueryEngine(ExecutorService service) {
        this.service = service;
    }
    public void execute(Operator operator) {
        service.execute(operator);
    }
}
