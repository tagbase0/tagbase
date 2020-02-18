package com.oppo.tagbase.meta;

import com.oppo.tagbase.meta.connector.MetadataConnector;
import com.oppo.tagbase.meta.obj.Dict;

import javax.inject.Inject;

/**
 * Metadata service for job
 *
 * Created by wujianchao on 2020/2/17.
 */
public class MetadataDict {

    @Inject
    private MetadataConnector metadataConnector;

    public void addDict(Dict dict) {
        metadataConnector.addDict(dict);
    }

    public Dict getDict() {
        return metadataConnector.getDict();
    }

    public long getDictElementCount() {
        return metadataConnector.getDictElementCount();
    }
}
