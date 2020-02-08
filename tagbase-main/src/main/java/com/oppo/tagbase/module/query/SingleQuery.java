package com.oppo.tagbase.module.query;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by 71518 on 2020/2/7.
 */
public class SingleQuery implements  Query {


    @JsonProperty("dataSource")
    private String datasource;

    private String output;


    private List<String> dimensions;


    private List<Filter> filters;


    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }
    public String getDatasource() {
        return datasource;
    }

    public void setOutput(String output) {
        this.output = output;
    }
    public String getOutput() {
        return output;
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

}