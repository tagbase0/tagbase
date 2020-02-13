package com.oppo.tagbase.meta.connector;

/**
 * Created by wujianchao on 2020/2/4.
 */
public class MySQLMetadataConnector extends MetadataConnector {

    public MySQLMetadataConnector() throws ClassNotFoundException {
        super();
    }

    @Override
    protected void registerJDBCDriver() throws ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
    }

}
