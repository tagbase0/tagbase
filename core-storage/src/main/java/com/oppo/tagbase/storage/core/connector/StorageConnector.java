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
        //从元数据slice获取分片数,默认为1
        int partition = 1;
        createTable(dbName, tableName, partition);
    }

    protected abstract void createTable(String dbName, String tableName, int partition) ;

    public abstract void deleteTable(String dbName, String tableName) ;

    public abstract void createRecord(String dbName, String tableName, String key, ImmutableRoaringBitmap value) ;

    public abstract void createBatchRecords(String dbName, String tableName, String dataPath) ;

    public OperatorBuffer createQuery(SingleQueryManager queryManager) {

        Table metaTable = meta.getTable(queryManager.getDbName(), queryManager.getDbName());
        List<SliceSegment> sliceList = getSliceSegments(metaTable, queryManager);
        OperatorBuffer buffer = new OperatorBuffer(sliceList.size());
        //假如没有slice符合查询条件，直接返回
        if(sliceList.size() == 0){
            log.debug("there is no need to scan hbase, because no slice is suitable ");
            buffer.offer(AggregateRow.EOF);
            return buffer;
        }
        List<DimensionContext> dimensionContextList = getDimensionContexts(metaTable, queryManager);

        for (SliceSegment slice : sliceList){
            int totalShard = slice.getTotalShard();
            //默认totalShard是只有1个
            for(int i=1; i<=totalShard; i++){
                slice.setSegmentId(i);
                StorageQueryContext storageQueryContext = new StorageQueryContext(dimensionContextList, slice);
                queryExecutor.execute(new QueryTask(storageQueryContext, buffer));
            }
        }
        return buffer;
    }

    private List<DimensionContext>  getDimensionContexts(Table metaTable, SingleQueryManager query){
        Map<String, DimensionContext> dimContextMap = new HashMap<>();

        String sliceColumnName = "defaultSliceColumn";
        if(query.hasSliceQuery()) {
            sliceColumnName = query.getSliceQuery().getColumn();
        }
        dimContextMap.put(sliceColumnName, new DimensionContext(StorageConstantUtil.FLAG_SLICE_COLUMN_INDEX, sliceColumnName));

        metaTable.getColumns().stream().filter(column -> column.getType()== ColumnType.DIM_COLUMN)
        .forEach(column -> dimContextMap.put(column.getName(), new DimensionContext(column.getIndex(), column.getName())));

        if(query.hasdimQuery()){
            query.getDimQueryList().stream().filter(dimQuery -> (dimQuery instanceof InQuery && dimContextMap.containsKey(dimQuery.getColumn())))
                    .forEach(dimQuery -> {
                        List<String> dimValues = ((InQuery) dimQuery).getColumnRange().asRanges()
                                .stream().map(range -> range.lowerEndpoint()).collect(Collectors.toList());
                        dimContextMap.get(dimQuery.getColumn()).setDimValues(dimValues);
                    });
        }

        int returnIndex = 1;
        if(query.getDimensions() != null) {
            for (String dimName : query.getDimensions()) {
                if (dimContextMap.containsKey(dimName)) {
                    dimContextMap.get(dimName).setDimReturnIndex(returnIndex);
                }
                returnIndex++;
            }
        }

        List<DimensionContext> dimensionContextList = dimContextMap.values().stream()
                .sorted(new DimensionComparator()).collect(Collectors.toList());

        return dimensionContextList;
    }


    private List<SliceSegment> getSliceSegments(Table metaTable, SingleQueryManager queryManager){
        List<SliceSegment> sliceSegments = new ArrayList<>();
        List<Slice> sliceList = null;
        switch (metaTable.getType()){
            case TAG:
                sliceList = meta.getSlices(queryManager.getDbName(), queryManager.getTableName());
                break;
            case ACTION:
                if(queryManager.hasSliceQuery()){
                    sliceList = meta.getSlices(queryManager.getDbName(), queryManager.getTableName(), queryManager.getSliceQuery().getSliceRange());

                }else {
                    sliceList = meta.getSlices(queryManager.getDbName(), queryManager.getTableName());
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
            } catch (IOException e) {
                log.error("QueryTask createStorageQuery error", e);
            }
            log.debug("finish QueryTask, Thread name:" + Thread.currentThread().getName());
        }
    }

}
