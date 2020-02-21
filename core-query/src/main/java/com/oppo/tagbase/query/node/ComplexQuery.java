package com.oppo.tagbase.query.node;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.List;
/**
 * Created by huangfeng on 2020/2/15.
 */

public class ComplexQuery implements Query{

    private OutputType outputType;

    @JsonProperty("operation")
    private OperatorType operation;

    @JsonProperty("subqueryList")
    private List<Query> subQueries;

    @JsonCreator
    public ComplexQuery(
            @JsonProperty("operation") OperatorType operator,
            @JsonProperty("subqueryList") List<Query> subQueries,
            @JsonProperty("output") OutputType outputType
    ) {
        Preconditions.checkNotNull(operator, "operator  can not be null");
        Preconditions.checkNotNull(outputType, "output  can not be null");

        if(subQueries == null || subQueries.size() == 1){
            throw new  IllegalArgumentException("size of subQueryList must be greater than 1");
        }

        this.operation = operator;
        this.outputType = outputType;

    }


    public List<Query> getSubQueries() {
        return subQueries;
    }

    public OutputType getOutputType() {
        return outputType;
    }


    public OperatorType getOperation() {
        return operation;
    }

    @Override
    public OutputType getOutput() {
        return null;
    }

    @Override
    public <R> R accept(QueryVisitor<R> visitor) {
        return visitor.visitComplexQuery(this);
    }

}