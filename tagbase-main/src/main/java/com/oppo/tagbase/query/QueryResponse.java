package com.oppo.tagbase.query;

import java.util.List;
/**
 * @author huangfeng
 * @date 2020/2/9
 */
public class QueryResponse {
    String code;
    String message;
    List<String> data;

    public static QueryResponse error(Exception e) {
        return null;

    }
}
