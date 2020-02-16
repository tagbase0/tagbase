package com.oppo.tagbase.storage.core.example.testobj;

/**
 * @author huangfeng
 * @date 2020/2/7
 */


public interface Filter {
//    CompareEnum getOperator();
    String getColumn();
//    List<String> getValue();
    boolean isExact();
}