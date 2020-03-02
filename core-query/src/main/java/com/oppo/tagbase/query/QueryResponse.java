package com.oppo.tagbase.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.oppo.tagbase.common.TagbaseException;

import java.util.List;
import java.util.Map;

/**
 * @author huangfeng
 * @date 2020/2/9
 */
public final class QueryResponse {
    int code;
    String message;
    List<Map<String, Object>> data;

    private QueryResponse(int code, String message) {
        this.code = code;
        this.message = message;
    }

    private QueryResponse(List<Map<String, Object>> data) {
        this.data = data;

    }

    public static QueryResponse error(Exception e) {
        if(e instanceof TagbaseException){
            return new QueryResponse(((TagbaseException) e).getErrorCode().getCode(),e.getMessage());
        }

        return new QueryResponse(500, e.getMessage());
    }
    public static QueryResponse queryId(String queryId) {
        return new QueryResponse(ImmutableList.of(ImmutableMap.of("queryId", queryId)));
    }

    public static QueryResponse fillContent(Object call) {
        return null;
    }
}
