package com.oppo.tagbase.storage.core.connector;

import com.google.inject.Inject;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.obj.ColumnType;
import com.oppo.tagbase.meta.obj.Slice;
import com.oppo.tagbase.meta.obj.Table;
import com.oppo.tagbase.storage.core.obj.*;
import com.oppo.tagbase.storage.core.util.StorageConstantUtil;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by liangjingya on 2020/2/8.
 */
public abstract class StorageConnector {

    private ExecutorService queryExecutor = null;

    @Inject
    private Metadata meta;
//    private TestMetadata meta;

    @Inject
    private StorageConnectorConfig commonConfig;

    protected Logger log = LoggerFactory.getLogger(StorageConnector.class);

    public void init(){
        log.info(commonConfig.toString());
        synchronized (StorageConnector.class) {
            if (queryExecutor == null) {
                int maxThreads = commonConfig.getQueryPoolMaxThread();
                int coreThreads = commonConfig.getQueryPoolCoreThread();
                long keepAliveTime =commonConfig.getQueryPoolKeepAliveSecond();
                LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(commonConfig.getQueryPoolQueueSie());
                queryExecutor = new ThreadPoolExecutor(coreThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS, workQueue);
            }
        }
        initConnector();
    }

    protected abstract void initConnector();

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
            e.printStackTrace();
        }
        destroyConnector();
    }

    protected abstract void destroyConnector();

    public void createTable(String dbName, String tableName) {
        //从元数据slice获取分片数,目前默认为1
        int partition = 1;
        createTable(dbName, tableName, partition);
    }

    protected abstract void createTable(String dbName, String tableName, int partition) ;

    public abstract void deleteTable(String dbName, String tableName) ;

    public abstract void createRecord(String dbName, String tableName, String key, ImmutableRoaringBitmap value) ;

    public abstract void createBatchRecords(String dbName, String tableName, String dataPath) ;

    public OperatorBuffer createQuery(QueryHandler queryHandler) {

        Table metaTable = meta.getTable(queryHandler.getDbName(), queryHandler.getDbName());
        List<SliceSegment> sliceList = getSliceSegments(metaTable, queryHandler);
        OperatorBuffer buffer = new OperatorBuffer(sliceList.size());
        //假如没有slice符合查询条件，直接返回
        if(sliceList.size() == 0){
            log.debug("there is no need to scan hbase, because no slice is suitable ");
            buffer.offer(AggregateRow.EOF);
            return buffer;
        }
        List<DimContext> dimContextList = getDimContexts(metaTable, queryHandler);

        for (SliceSegment slice : sliceList){
            //目前默认totalShard是只有1个
            //int totalShard = slice.getTotalShard();
            int totalShard = 1;
            for(int i=1; i<=totalShard; i++){
                slice.setSegmentId(i);
                StorageQueryContext storageQueryContext = new StorageQueryContext(dimContextList, slice);
                queryExecutor.execute(new QueryTask(storageQueryContext, buffer));
            }
        }
        return buffer;
    }

    private List<DimContext>  getDimContexts(Table metaTable, QueryHandler queryHandler){
        Map<String, DimContext> dimContextMap = new HashMap<>();

        String sliceColumnName = "defaultSliceColumn";
        if(queryHandler.hasSliceColumn()) {
            sliceColumnName = queryHandler.getSliceColumn().getColumnName();
        }
        dimContextMap.put(sliceColumnName, new DimContext(StorageConstantUtil.FLAG_SLICE_COLUMN_INDEX, sliceColumnName));

        metaTable.getColumns().stream().filter(column -> column.getType()== ColumnType.DIM_COLUMN)
        .forEach(column -> dimContextMap.put(column.getName(), new DimContext(column.getIndex(), column.getName())));

        if(queryHandler.hasDimColumnList()){
            queryHandler.getDimColumnList().stream().filter(dimColumn -> dimContextMap.containsKey(dimColumn.getColumnName()))
                    .forEach(dimColumn -> {
                        List<String> dimValues = dimColumn.getColumnRange().asRanges()
                                .stream().map(range -> range.lowerEndpoint()).collect(Collectors.toList());
                        dimContextMap.get(dimColumn.getColumnName()).setDimValues(dimValues);
                    });
        }

        int returnIndex = 1;
        if(queryHandler.getDimensions() != null) {
            for (String dimName : queryHandler.getDimensions()) {
                if (dimContextMap.containsKey(dimName)) {
                    dimContextMap.get(dimName).setDimReturnIndex(returnIndex);
                }
                returnIndex++;
            }
        }

        List<DimContext> dimContextList = dimContextMap.values().stream()
                .sorted(new DimComparator()).collect(Collectors.toList());

        return dimContextList;
    }


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
            sliceList.stream().forEach(slice -> sliceSegments.add(new SliceSegment(slice.getStartTime().toString(), slice.getSink(), slice.getShardNum())));
        }

        return sliceSegments;
    }

    protected abstract void createStorageQuery(StorageQueryContext storageQueryContext, OperatorBuffer buffer) throws IOException;

    class QueryTask implements Runnable {

        private OperatorBuffer buffer;
        private StorageQueryContext storageQueryContext;

        QueryTask(StorageQueryContext storageQueryContext, OperatorBuffer buffer) {
            this.buffer = buffer;
            this.storageQueryContext = storageQueryContext;
        }

        @Override
        public void run() {
            log.debug("start QueryTask, Thread name:" + Thread.currentThread().getName());
            try {
                createStorageQuery(storageQueryContext, buffer);
            } catch (Exception e) {
                log.error("QueryTask createStorageQuery error", e);
            }
            log.debug("finish QueryTask, Thread name:" + Thread.currentThread().getName());
        }
    }

}
