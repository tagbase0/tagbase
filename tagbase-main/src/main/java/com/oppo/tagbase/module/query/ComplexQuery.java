package com.oppo.tagbase.module.query;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;


public class ComplexQuery implements Query{


    private String output;

    @JsonProperty("operation")
    private String operation;

    @JsonProperty("subqueryList")
    private List<Query> subQueries;


    public void setOutput(String output) {
        this.output = output;
    }
    public String getOutput() {
        return output;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }
    public String getOperation() {
        return operation;
    }

    public void setSubQueries(List<Query> subQueries) {
        this.subQueries = subQueries;
    }
    public List<Query> getSubQueries() {
        return subQueries;
    }

}