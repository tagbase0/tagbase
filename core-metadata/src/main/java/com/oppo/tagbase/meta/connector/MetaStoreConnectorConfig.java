package com.oppo.tagbase.meta.connector;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.oppo.tagbase.common.guice.Config;

/**
 * Created by wujianchao on 2020/2/4.
 */
@Config("tagbase.metadata.storage.connector")
public class MetaStoreConnectorConfig {

    @JsonProperty("uri")
    private String connectURI;

    @JsonProperty("user")
    private String user;

    @JsonProperty("password")
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
