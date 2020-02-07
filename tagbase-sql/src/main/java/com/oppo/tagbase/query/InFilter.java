package com.oppo.tagbase.query;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by 71518 on 2020/2/7.
 */
public class InFilter implements  Filter {

    @JsonProperty("dimName")
    private String dimname;
    private List<String> values;


    public void setDimname(String dimname) {
        this.dimname = dimname;
    }
    public String getDimname() {
        return dimname;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }
    public List<String> getValues() {
        return values;
    }


}
