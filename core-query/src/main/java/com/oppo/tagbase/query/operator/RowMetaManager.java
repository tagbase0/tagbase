package com.oppo.tagbase.query.operator;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by huangfeng on 2020/2/15.
 */
public class RowMetaManager {
    int metaId;
    Map<String, RowMeta> metaCache = new HashMap<>();

    public RowMeta combine(int id1, int id2) {

        String key = id1+","+id2;
        if (metaCache.containsKey(key)) {
            synchronized (this) {
                metaId++;
                metaCache.put(key, metaCache.get(id1).combine(metaCache.get(id2)));

            }
        }

        return metaCache.get(key);
    }


}
