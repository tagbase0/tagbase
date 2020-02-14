package com.oppo.tagbase.query.node;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by 71518 on 2020/2/7.
 */
public class InFilter implements  Filter {

    @JsonProperty("dimName")
    private String dimName;
    private List<String> values;


    public void setDimName(String dimName) {
        this.dimName = dimName;
    }
    public String getDimName() {
        return dimName;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }
    public List<String> getValues() {
        return values;
    }

    @Override
    public String getColumn() {
        return dimName;
    }

    @Override
    public boolean isExact() {
        return values.size() == 1;
    }
}
