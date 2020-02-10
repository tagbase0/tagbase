package com.oppo.tagbase.query.node;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;


public class ComplexQuery implements Query{


    private OutputType output;

    @JsonProperty("operation")
    private Operator operation;

    @JsonProperty("subqueryList")
    private List<Query> subQueries;






    public void setSubQueries(List<Query> subQueries) {
        this.subQueries = subQueries;
    }
    public List<Query> getSubQueries() {
        return subQueries;
    }

    @Override
    public <R> R accept(QueryVisitor<R> visitor) {
        return visitor.visitComplexQuery(this);
    }

    public OutputType getOutput() {
        return output;
    }

    public  enum Operator{
        intersect,
        union,
        diff
    }


}