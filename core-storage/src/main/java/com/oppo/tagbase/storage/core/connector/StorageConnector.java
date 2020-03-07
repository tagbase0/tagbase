package com.oppo.tagbase.storage.core.connector;

import com.google.inject.Inject;
import com.oppo.tagbase.common.guice.Extension;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.obj.ColumnType;
import com.oppo.tagbase.meta.obj.Slice;
import com.oppo.tagbase.meta.obj.Table;
import com.oppo.tagbase.storage.core.exception.StorageException;
import com.oppo.tagbase.storage.core.executor.StorageExecutors;
import com.oppo.tagbase.storage.core.obj.*;
import com.oppo.tagbase.storage.core.util.StorageConstant;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Created by liangjingya on 2020/2/8.
 */
@Extension(key = "tagbase.storage.type", defaultImpl = "hbase")
public abstract class StorageConnector {

    private ExecutorService queryExecutor = null;

    @Inject
    private Metadata meta;

    @Inject
    private StorageConnectorConfig commonConfig;

    protected Logger log = LoggerFactory.getLogger(StorageConnector.class);

    /**
     * init query thread pool and connector
     */
        public void init(){
        log.info(commonConfig.toString());
        synchronized (StorageConnector.class) {
            if (queryExecutor == null) {
                int maxThreads = commonConfig.getQueryPoolMaxThread();
                int coreThreads = commonConfig.getQueryPoolCoreThread();
                long keepAliveTime =commonConfig.getQueryPoolKeepAliveSecond();
                int queueSize = commonConfig.getQueryPoolQueueSie();
                queryExecutor = StorageExecutors.newThreadPool(coreThreads, maxThreads, keepAliveTime, queueSize);
            }
        }
        initConnector();
    }

    protected abstract void initConnector() throws StorageException ;

    /**
     * destroy query thread pool and connector
     */
    public void destroy(){
        if (queryExecutor == null) {
            return;
        }
        queryExecutor.shutdownNow();
        try {
            if (!queryExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                queryExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("destroy storage error", e);
        }
        destroyConnector();
    }

    protected abstract void destroyConnector();

    /**
     * create storage table
     */
    public void createTable(String dbName, String tableName) throws StorageException {
        //从元数据slice获取分片数,目前默认为1
        int partition = 1;
        createTable(dbName, tableName, partition);
    }

    protected abstract void createTable(String dbName, String tableName, int partition) throws StorageException;

    /**
     * delete storage table
     */
    public abstract void deleteTable(String dbName, String tableName) throws StorageException;

    /**
     * insert one record into storage
     */
    public abstract void addRecord(String dbName, String tableName, String key, ImmutableRoaringBitmap value) throws StorageException;

    /**
     * insert multiple records into storage
     */
    public abstract String addSlice(String dataPath) throws StorageException;

    /**
     * query interface
     */
    public OperatorBuffer<RawRow> createQuery(QueryHandler queryHandler) {

        Table metaTable = meta.getTable(queryHandler.getDbName(), queryHandler.getTableName());
        List<SliceSegment> sliceList = getSliceSegments(metaTable, queryHandler);
        OperatorBuffer<RawRow> buffer = new OperatorBuffer<>(sliceList.size());
        //假如没有slice符合查询条件，直接返回
        if(sliceList.size() == 0){
            log.debug("there is no need to scan hbase, because no slice is suitable ");
            buffer.postEnd();
            return buffer;
        }
        List<DimContext> dimContextList = getDimContexts(metaTable, queryHandler);

        for (SliceSegment slice : sliceList){
            //目前默认totalShard是只有1个
            //int totalShard = slice.getTotalShard();
            int totalShard = 1;
            for(int i=1; i<=totalShard; i++){
                slice.setSegmentId(i);
                StorageQueryContext storageQueryContext = new StorageQueryContext(dimContextList, slice, queryHandler.getQueryId());
                queryExecutor.execute(new QueryTask(storageQueryContext, buffer));
            }
        }
        return buffer;
    }

    /**
     * get dimension context by queryHandler
     */
    private List<DimContext>  getDimContexts(Table metaTable, QueryHandler queryHandler){
        Map<String, DimContext> dimContextMap = new HashMap<>();

        metaTable.getColumns().stream()
                .filter(column -> column.getType()== ColumnType.DIM_COLUMN)
                .forEach(column -> dimContextMap.put(column.getSrcName(), new DimContext(column.getIndex(), column.getSrcName(), ColumnType.DIM_COLUMN)));

        if(queryHandler.hasFilterColumnList()){
            queryHandler.getFilterColumnList().stream()
                    .filter(dimColumn -> dimContextMap.containsKey(dimColumn.getColumnName()))
                    .forEach(dimColumn -> {
                        dimContextMap.get(dimColumn.getColumnName()).setDimValues(dimColumn.getColumnRange());
                    });
        }

        String sliceColumnName= null;
        DimContext sliceDimContext = null;
        if(queryHandler.hasSliceColumn()) {
            sliceColumnName = queryHandler.getSliceColumn().getColumnName();
            sliceDimContext = new DimContext(StorageConstant.SLICE_COLUMN_INDEX, sliceColumnName, ColumnType.SLICE_COLUMN);
        }

        int returnIndex = 1;
        if(queryHandler.getDimensions() != null) {
            for (String dimName : queryHandler.getDimensions()) {
                if(sliceColumnName != null && dimName.equals(sliceColumnName)){
                    sliceDimContext.setDimReturnIndex(returnIndex);
                }else if (dimContextMap.containsKey(dimName)) {
                    dimContextMap.get(dimName).setDimReturnIndex(returnIndex);
                }
                returnIndex++;
            }
        }

        List<DimContext> dimContextList = dimContextMap.values().stream()
                .sorted(new DimComparator())
                .collect(Collectors.toList());

        if(sliceDimContext != null){
            dimContextList.add(sliceDimContext);
        }

        return dimContextList;
    }

    /**
     * get Slices by queryHandler
     */
    private List<SliceSegment> getSliceSegments(Table metaTable, QueryHandler queryHandler){
        List<SliceSegment> sliceSegments = new ArrayList<>();
        List<Slice> sliceList = null;
        switch (metaTable.getType()){
            case TAG:
                sliceList = meta.getSlices(queryHandler.getDbName(), queryHandler.getTableName());
                break;
            case ACTION:
                if(queryHandler.hasSliceColumn()){
                    sliceList = meta.getSlices(queryHandler.getDbName(), queryHandler.getTableName(), queryHandler.getSliceColumn().getColumnRange());

                }else {
                    sliceList = meta.getSlices(queryHandler.getDbName(), queryHandler.getTableName());
                }
                break;
            default:
                break;
        }
        if(sliceList != null) {
            sliceList.stream()
                    .forEach(slice -> sliceSegments.add(new SliceSegment(slice.getStartTime().toString(), slice.getSink(), slice.getShardNum())));
        }

        return sliceSegments;
    }

    protected abstract void createStorageQuery(StorageQueryContext storageQueryContext, OperatorBuffer<RawRow> buffer) throws StorageException;

    /**
     * Runnable query task
     */
    class QueryTask implements Runnable {

        private OperatorBuffer<RawRow> buffer;
        private StorageQueryContext storageQueryContext;

        QueryTask(StorageQueryContext storageQueryContext, OperatorBuffer<RawRow> buffer) {
            this.buffer = buffer;
            this.storageQueryContext = storageQueryContext;
        }

        @Override
        public void run() {
            String originThreadName = Thread.currentThread().getName();
            String newThreadName = originThreadName + "-" + storageQueryContext.getQueryId();
            //修改线程名，添加queryId
            Thread.currentThread().setName(newThreadName);
            log.debug("start QueryTask, {}", storageQueryContext);
            try {
                createStorageQuery(storageQueryContext, buffer);
            } catch (Exception e) {
                log.error("QueryTask createStorageQuery error", e);
            }
            log.debug("finish QueryTask");
            Thread.currentThread().setName(originThreadName);
        }
    }

}
