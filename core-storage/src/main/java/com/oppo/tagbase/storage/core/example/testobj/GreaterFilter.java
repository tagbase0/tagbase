package com.oppo.tagbase.storage.core.example.testobj;

/**
 * @author huangfeng
 * @date 2020/2/7
 */

public class GreaterFilter implements Filter {



    private String dimName;


    private String value;

    public GreaterFilter(String dimName, String value) {
        this.dimName = dimName;
        this.value = value;
    }

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

//    @Override
//    public CompareEnum getOperator() {
//        return CompareEnum.GREATER;
//    }

    @Override
    public String getColumn() {
        return dimName;
    }

//    @Override
//    public List<String> getValue() {
//        List<String> v = new ArrayList<>();
//        v.add(value);
//        return v;
//    }

    @Override
    public boolean isExact() {
        return false;
    }
}
