package com.oppo.tagbase.query;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author huangfeng
 * @date 2020/2/9
 */
public class QueryManager {

    Map<String,QueryExecution> queries;

    QueryManager(){
        queries = new ConcurrentHashMap<>();

    }


    public void register(String id, QueryExecution execution){

        queries.put(id,execution);
    }




}
