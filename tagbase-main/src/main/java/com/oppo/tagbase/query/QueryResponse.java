package com.oppo.tagbase.query;

import java.util.Map;

/**
 * @author huangfeng
 * @date 2020/2/9
 */
public final class QueryResponse {


    int code;
    String message;
    Map<String,Integer> data;

    private QueryResponse(int code,String message){
        this.code = code;
        this.message = message;
    }

    public static QueryResponse error(Exception e) {
        return new QueryResponse(500,e.getMessage());

    }





}
