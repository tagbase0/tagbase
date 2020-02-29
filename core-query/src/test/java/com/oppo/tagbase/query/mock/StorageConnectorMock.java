package com.oppo.tagbase.query.mock;

import com.google.common.collect.ImmutableList;
import com.oppo.tagbase.storage.core.connector.StorageConnector;
import com.oppo.tagbase.storage.core.exception.StorageException;
import com.oppo.tagbase.storage.core.obj.*;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.util.List;

/**
 * @author huangfeng
 * @date 2020/2/25 21:27
 */
public class StorageConnectorMock extends StorageConnector {
    int index=0;
    List<OperatorBuffer> outputBuffer;
    public StorageConnectorMock(List<RawRow> dataset ){
        ImmutableList.Builder<OperatorBuffer> outputBufferBuilder = ImmutableList.builder();

        Dimensions preDim = dataset.get(0).getDim();

        OperatorBuffer operatorBuffer = new OperatorBuffer();
        for(int n=0;n<dataset.size();n++){

            if(dataset.get(n).getDim().length() != preDim.length()){
                operatorBuffer.postEnd();
                outputBufferBuilder.add(operatorBuffer);
                operatorBuffer = new OperatorBuffer();
                preDim = dataset.get(n).getDim();
            }

            operatorBuffer.postData(dataset.get(n));
        }
//        dataset.forEach(row -> outputBuffer.postData(row));
        operatorBuffer.postEnd();
        outputBufferBuilder.add(operatorBuffer);
        outputBuffer=  outputBufferBuilder.build();
    }

    public OperatorBuffer createQuery(QueryHandler queryHandler) {
        return outputBuffer.get(index++);
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
    protected void createStorageQuery(StorageQueryContext storageQueryContext, OperatorBuffer buffer) throws  StorageException {

    }
}
