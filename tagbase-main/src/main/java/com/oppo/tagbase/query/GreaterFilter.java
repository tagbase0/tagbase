package com.oppo.tagbase.query;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by 71518 on 2020/2/7.
 */
public class GreaterFilter implements Filter {


    @JsonProperty("dimName")
    private String dimName;


    private String value;


    public void setDimname(String dimname) {
        this.dimName = dimname;
    }
    public String getDimname() {
        return dimName;
    }

    public void setValues(String values) {
        this.value = values;
    }
    public String getValues() {
        return value;
    }
}
