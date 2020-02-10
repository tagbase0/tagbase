package com.oppo.tagbase.storage.connector;

import com.oppo.tagbase.storage.bean.BitmapBean;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.IOException;
import java.util.Iterator;

//import com.oppo.tagbase.query.node.SingleQuery;

/**
 * Created by liangjingya on 2020/2/8.
 */
public interface StorageConnector {

    void initConnector();

    void destroyConnector();

    boolean createTable(String tableName) throws IOException;

    boolean deleteTable(String tableName) throws IOException;

    void addRecord(String tableName, String key, ImmutableRoaringBitmap value) throws IOException;

    void bulkLoadRecords(String dataPath);

//    Iterator<BitmapBean> getRecords(String tableName, SingleQuery query);

    Iterator<BitmapBean> getRecords(String tableName) throws IOException;

}
