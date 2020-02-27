package com.oppo.tagbase.query.mock;

import com.oppo.tagbase.storage.core.connector.StorageConnector;
import com.oppo.tagbase.storage.core.exception.StorageException;
import com.oppo.tagbase.storage.core.obj.RawRow;
import com.oppo.tagbase.storage.core.obj.OperatorBuffer;
import com.oppo.tagbase.storage.core.obj.QueryHandler;
import com.oppo.tagbase.storage.core.obj.StorageQueryContext;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.IOException;
import java.util.List;

/**
 * @author huangfeng
 * @date 2020/2/25 21:27
 */
public class StorageConnectorMock extends StorageConnector {

    OperatorBuffer outputBuffer;
    public StorageConnectorMock(List<RawRow> dataset ){
        outputBuffer = new OperatorBuffer(1);
        dataset.forEach(row -> outputBuffer.postData(row));
    }

    public OperatorBuffer createQuery(QueryHandler queryHandler) {
        return outputBuffer;
    }

    @Override
    protected void initConnector() {

    }

    @Override
    protected void destroyConnector() {

    }

    @Override
    protected void createTable(String dbName, String tableName, int partition) throws StorageException {

    }

    @Override
    public void deleteTable(String dbName, String tableName) throws StorageException {

    }

    @Override
    public void createRecord(String dbName, String tableName, String key, ImmutableRoaringBitmap value) throws StorageException {

    }

    @Override
    public void createBatchRecords(String dbName, String tableName, String dataPath) throws StorageException {

    }

    @Override
    protected void createStorageQuery(StorageQueryContext storageQueryContext, OperatorBuffer buffer) throws IOException, StorageException {

    }
}
