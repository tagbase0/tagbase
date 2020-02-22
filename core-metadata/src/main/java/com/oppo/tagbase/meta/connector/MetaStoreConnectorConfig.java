package com.oppo.tagbase.meta.connector;

import com.oppo.tagbase.common.guice.Config;

/**
 * Created by wujianchao on 2020/2/4.
 */
@Config("tagbase.metadata.storage.connector")
public class MetaStoreConnectorConfig {

    private String connectURI;
    private String user;
    private String password;

    public String getConnectURI() {
        return connectURI;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
