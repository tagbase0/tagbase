package com.oppo.tagbase.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.oppo.tagbase.query.operator.Operator;
import org.javatuples.Pair;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
//        OperatorBuffer output = getOutputOperator().getOutputBuffer();
        return getOutputOperator().getOutputBuffer().next();
    }

    public void cancel() {
        for (Operator operator : operators) {
            operator.cancelOutput();
        }
    }

    private Operator getOutputOperator() {
        return operators.get(operators.size() - 1);
    }

    public List<Operator> getOperators() {
        return operators;
    }


    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        Operator outputOperator = getOutputOperator();
        LinkedList<Pair<Integer,Operator>> stack = new LinkedList<>();
        builder.append("PhysicalPlan{\n");

        stack.push(Pair.with(3,outputOperator));
        while(!stack.isEmpty()) {
            Pair<Integer,Operator>  p = stack.pop();
            final Operator curOperator = p.getValue1();
            int indent = p.getValue0();

            builder.append(String.join("", Collections.nCopies(indent, " ")));
            builder.append(p.getValue1().toString());
            builder.append("\n");

            List<Integer> childId = operatorMapping.entrySet().stream().
                    filter(entry -> entry.getValue() == curOperator.getId()).
                    map(Map.Entry::getKey).collect(Collectors.toList());

            operators.stream().filter(operator -> childId.contains(operator.getId())).map(item -> Pair.with(indent + 3, item)).
                    forEach(pair -> stack.push(pair));

        }

        builder.append("}");

        return  builder.toString();
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
