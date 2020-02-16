package com.oppo.tagbase.storage.core.example.testobj;

import java.util.List;

/**
 * Created by 71518 on 2020/2/7.
 */
public class InFilter implements Filter {


    private String dimName;
    private List<String> values;

    public InFilter(String dimName, List<String> values) {
        this.dimName = dimName;
        this.values = values;
    }

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

//    @Override
//    public CompareEnum getOperator() {
//        return CompareEnum.IN;
//    }

    @Override
    public String getColumn() {
        return dimName;
    }

//    @Override
//    public List<String> getValue() {
//        return values;
//    }

    @Override
    public boolean isExact() {
        return values.size() == 1;
    }
}
