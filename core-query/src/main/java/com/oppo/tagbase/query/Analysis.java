package com.oppo.tagbase.query;

import com.oppo.tagbase.meta.obj.Table;
import com.oppo.tagbase.query.node.Query;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author huangfeng
 * @date 2020/2/18 14:36
 */
public class Analysis {

    private Map<Query, Scope> scopes = new HashMap<>();

    private Map<Query, String> queryDB = new HashMap<>();

    private Map<Query, Table> queryTables = new HashMap<>();


    private Map<Query, List<String>> queryDims = new HashMap<>();

    private Map<Query, Map<String, FilterAnalysis>> queryFilterAnalysis = new HashMap<>();





    public void addDB(Query query, String db) {
        queryDB.put(query, db);
    }

    public void addTable(Query query, Table table) {
        queryTables.put(query, table);
    }

    public void addFilterAnalysis(Query query, Map<String, FilterAnalysis> filterAnalysis) {
        queryFilterAnalysis.put(query, filterAnalysis);

    }

    public void addScope(Query query, Scope scope) {
        scopes.put(query, scope);
    }


    public Scope getScope(Query query) {
        return scopes.get(query);
    }



    public List<String> getDims(Query query) {
        return queryDims.get(query);
    }

    public Table getQueryTable(Query query) {
        return queryTables.get(query);
    }

    public String getQueryDB(Query query) {
        return queryDB.get(query);
    }

    public Map<String, FilterAnalysis> getQueryFilterAnalysis(Query query) {
        return queryFilterAnalysis.get(query);
    }

}
