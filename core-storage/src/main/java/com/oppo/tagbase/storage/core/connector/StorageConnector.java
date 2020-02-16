package com.oppo.tagbase.storage.core.connector;

import com.google.inject.Inject;
import com.oppo.tagbase.storage.core.example.testobj.*;
import com.oppo.tagbase.storage.core.obj.ComparatorFilterMeta;
import com.oppo.tagbase.storage.core.obj.FilterMeta;
import com.oppo.tagbase.storage.core.obj.ScanMeta;
import com.oppo.tagbase.storage.core.obj.SlicePartitionMeta;
import com.oppo.tagbase.storage.core.util.ConstantStorageUtil;
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

    protected Logger log = LoggerFactory.getLogger(StorageConnector.class);

    public void init(){
        synchronized (StorageConnector.class) {
            if (queryExecutor == null) {
                int maxThreads = 10;
                int coreThreads = 5;
                long keepAliveTime =60;
                LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(500);
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

    public void createTable(String tableName) throws IOException{
        //cong从元数据slice获取分片数
        int partition = 1;
        createTable(tableName, partition);
    }

    protected abstract void createTable(String tableName, int partition) throws IOException;

    public abstract void deleteTable(String tableName) throws IOException;

    public abstract void createRecord(String tableName, String key, ImmutableRoaringBitmap value) throws IOException;

    public abstract void createBatchRecords(String tableName, String dataPath) throws Exception;

    public OperatorBuffer createQuery(SingleQuery query) throws IOException{

//        //模拟meta模块获取到的数据
//        List<Column> columns = new ArrayList<>();
//        columns.add(new Column("version",3, ColumnType.DIM_COLUMN));
//        columns.add(new Column("app",1, ColumnType.DIM_COLUMN));
//        columns.add(new Column("event",2, ColumnType.DIM_COLUMN));
//        columns.add(new Column("imei",-1, ColumnType.BITMAP_COLUMN));
//        columns.add(new Column("daynum",-1, ColumnType.SLICE_COLUMN));
//        Table metaTable = new Table("event", TableType.ACTION, columns);
//        List<Slice> sliecs = new ArrayList<>();
//        sliecs.add(new Slice("20200210", "event_20200210", SliceStatus.READY));
//        sliecs.add(new Slice("20200209", "event_20200209", SliceStatus.READY));
//        sliecs.add(new Slice("20200211", "event_20200211", SliceStatus.READY));


        //模拟meta模块获取到的数据
//        List<Column> columns = new ArrayList<>();
//        columns.add(new Column("city",1, ColumnType.DIM_COLUMN));
//        Table metaTable = new Table("city", TableType.TAG, columns);
//        List<Slice> sliecs = new ArrayList<>();
//        sliecs.add(new Slice("", "city_20200210", SliceStatus.READY));
//        sliecs.add(new Slice("", "city_20200209", SliceStatus.DISABLED));


        Table metaTable = meta.getTable(query.getDbName(), query.getDbName());
        List<Slice> sliecs = meta.getSlices(query.getDbName(), query.getDbName());

        //获取daynum字段名
        String daynumDimension = null;
        if(metaTable.getType() == TableType.ACTION) {
            for (Column c : metaTable.getColumns()) {
                if (c.getType() == ColumnType.SLICE_COLUMN) {
                    daynumDimension = c.getName();
                    break;
                }
            }
        }

        List<SlicePartitionMeta> tableList = getTaleList(metaTable, sliecs, query, daynumDimension);
        OperatorBuffer buffer = new OperatorBuffer(tableList.size());
        //假如没有slice符合查询条件，直接返回
        if(tableList.size() == 0){
            log.debug("there is no need to scan hbase, because no slice is suitable ");
            buffer.offer(TagBitmap.EOF);
            return buffer;
        }
        List<FilterMeta>  filterMetaList = getFilterMetaList(metaTable, query, daynumDimension);

        for (SlicePartitionMeta slice : tableList){
            int shardNum = slice.getShardNum();
            for(int i=1; i<=shardNum; i++){
                slice.setPattition(i);
                ScanMeta scanMeta = new ScanMeta(filterMetaList, slice);
                queryExecutor.execute(new QueryTask(scanMeta, buffer));
            }
        }
        return buffer;
    }

    private List<FilterMeta>  getFilterMetaList(Table metaTable, SingleQuery query, String daynumDimension){
        //构造查询元数据
        Map<String, FilterMeta> filterMetaMap = new HashMap<>();
        filterMetaMap.put(daynumDimension, new FilterMeta(ConstantStorageUtil.FLAG_NO_NEED_RETURN_DIM, daynumDimension));//将daynum维度固定放在首位
        for(Column column : metaTable.getColumns()){
            if(column.getType() == ColumnType.DIM_COLUMN){
                filterMetaMap.put(column.getName(), new FilterMeta(column.getIndex(), column.getName()));
            }
        }
        for(Filter queryFilter : query.getFilters()){
            if(queryFilter.getColumn().equals(daynumDimension)) {
                continue;
            }
            if(queryFilter instanceof InFilter) {
                InFilter f = (InFilter) queryFilter;
                if(filterMetaMap.containsKey(f.getColumn())){
                    filterMetaMap.get(f.getColumn()).setValues(f.getValues());
                }
            }
        }

        int returnIndex = 1;
        if(query.getDimensions() != null) {
            for (String dim : query.getDimensions()) {
                if (filterMetaMap.containsKey(dim)) {
                    filterMetaMap.get(dim).setReturnIndex(returnIndex);
                }
                returnIndex++;
            }
        }

        List<FilterMeta> filterMetaList = filterMetaMap.values().stream()
                .sorted(new ComparatorFilterMeta()).collect(Collectors.toList());

        return filterMetaList;
    }

    private List<SlicePartitionMeta> getTaleList(Table metaTable, List<Slice> sliecs, SingleQuery query, String daynumDimension){
        //获取需要查询的hbase表
        List<SlicePartitionMeta> tableList = new ArrayList<>();
        switch (metaTable.getType()){
            case TAG:
                for(Slice slice : sliecs) {
                    if(slice.getStatus() == SliceStatus.READY) {
                        tableList.add(new SlicePartitionMeta(slice.getSink(),slice.getShardNum(),slice.getName()));
                    }
                }
                break;
            case ACTION:
                boolean hasDaynumFilter = false;
                for(Filter queryFilter : query.getFilters()) {
                    //如果含有daynum的filter，需要额外过滤选择slice
                    if (queryFilter.getColumn().equals(daynumDimension)) {
                        if(queryFilter instanceof InFilter) {
                            InFilter f = (InFilter)queryFilter;
                            for(String date : f.getValues()) {
                                for (Slice slice : sliecs) {
                                    if(date.equals(slice.getName()) && slice.getStatus() == SliceStatus.READY) {
                                        tableList.add(new SlicePartitionMeta(slice.getSink(),slice.getShardNum(),slice.getName()));
                                    }
                                }
                            }
                        }else if(queryFilter instanceof GreaterFilter) {
                            GreaterFilter f = (GreaterFilter)queryFilter;
                            String date = f.getValues();
                            for (Slice slice : sliecs) {
                                if(date.compareTo(slice.getName()) < 0 && slice.getStatus() == SliceStatus.READY){
                                    tableList.add(new SlicePartitionMeta(slice.getSink(),slice.getShardNum(),slice.getName()));
                                }
                            }

                        }else if(queryFilter instanceof BelowFilter) {
                            BelowFilter f = (BelowFilter)queryFilter;
                            String date = f.getValues();
                            for (Slice slice : sliecs) {
                                if(date.compareTo(slice.getName()) > 0 && slice.getStatus() == SliceStatus.READY){
                                    tableList.add(new SlicePartitionMeta(slice.getSink(),slice.getShardNum(),slice.getName()));
                                }
                            }
                        }
                        hasDaynumFilter = true;
                    }
                }
                //如果不含有daynum的filter，选择全部slice
                if(!hasDaynumFilter) {
                    for (Slice slice : sliecs) {
                        if(slice.getStatus() == SliceStatus.READY) {
                            tableList.add(new SlicePartitionMeta(slice.getSink(),slice.getShardNum(),slice.getName()));
                        }
                    }
                }
                break;
            default:
                break;
        }

        return tableList;
    }

    protected abstract void createQuery(ScanMeta scanMeta, OperatorBuffer buffer) throws IOException;

    class QueryTask implements Runnable {

        private OperatorBuffer buffer;
        private ScanMeta scanMeta;

        QueryTask(ScanMeta scanMeta, OperatorBuffer buffer) {
            this.buffer = buffer;
            this.scanMeta = scanMeta;
        }

        @Override
        public void run() {
            log.debug("start QueryTask, Thread name:" + Thread.currentThread().getName());
            try {
                createQuery(scanMeta, buffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            log.debug("finish QueryTask, Thread name:" + Thread.currentThread().getName());
        }
    }

}
