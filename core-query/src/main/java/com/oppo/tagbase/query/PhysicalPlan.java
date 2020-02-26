package com.oppo.tagbase.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.oppo.tagbase.query.operator.Operator;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author huangfeng
 * @date 2020/2/26 15:31
 */
public class PhysicalPlan {
    private List<Operator> operators;
    private String queryId;
    private Map<Integer, Integer> operatorMapping;


    public static PhysicalPlanBuilder builder() {
        return new PhysicalPlanBuilder();
    }

    public PhysicalPlan(String queryId, ImmutableList<Operator> operators, ImmutableMap<Integer, Integer> operatorMapping) {
        this.queryId = queryId;
        this.operators = operators;
        this.operatorMapping = operatorMapping;
    }


    public void ifFinish(Runnable callback) {
        getOutputOperator().ifFinish(callback);
    }

    public void ifException(Consumer<Exception> o) {
        for (Operator operator : operators) {
            operator.ifException(o);
        }

    }

    public Object getResult() {
        return getOutputOperator().getOutputBuffer().next();
    }

    public void cancel() {
        for (Operator operator : operators) {
            operator.cancelOutput();
        }
    }

    private Operator getOutputOperator(){
        return operators.get(operators.size() - 1);
    }

    public static class PhysicalPlanBuilder {
        private ImmutableList.Builder<Operator> operators = ImmutableList.builder();
        private String queryId;
        private ImmutableMap.Builder<Integer, Integer> operatorMapping = ImmutableMap.builder();

        public void setQueryId(String queryId) {
            this.queryId = queryId;
        }

        public void addOperator(int parentId, Operator operator) {
            //TODO it's too ugly ,but I has written.....
            operator.setQueryId(queryId);

            operators.add(operator);
            operatorMapping.put(operator.getId(), parentId);
        }

        public PhysicalPlan build() {
            return new PhysicalPlan(queryId, operators.build(), operatorMapping.build());
        }
    }
}
