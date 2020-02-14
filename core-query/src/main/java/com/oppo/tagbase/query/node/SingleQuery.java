package com.oppo.tagbase.query.node;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by 71518 on 2020/2/7.
 */
public class SingleQuery implements  Query {


    @JsonProperty("dataSource")
    private String datasource;

    private OutputType output;


    private List<String> dimensions;


    private List<Filter> filters;


    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }
    public String getDatasource() {
        return datasource;
    }


    public void setDimensions(List<String> dimensions) {
        this.dimensions = dimensions;
    }
    public List<String> getDimensions() {
        return dimensions;
    }

    public void setFilters(List<Filter> filters) {
        this.filters = filters;
    }
    public List<Filter> getFilters() {
        return filters;
    }

    public OutputType getOutput() {
        return output;
    }

    @Override
    public <R> R accept(QueryVisitor<R> visitor) {
        return visitor.visitSingleQuery(this);
    }
}