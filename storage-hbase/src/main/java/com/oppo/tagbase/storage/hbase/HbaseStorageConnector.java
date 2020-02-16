package com.oppo.tagbase.storage.hbase;

import com.oppo.tagbase.storage.core.connector.StorageConnector;
import com.oppo.tagbase.storage.core.example.testobj.OperatorBuffer;
import com.oppo.tagbase.storage.core.util.BitmapUtil;
import com.oppo.tagbase.storage.core.example.testobj.TagBitmap;
import com.oppo.tagbase.storage.core.obj.FilterMeta;
import com.oppo.tagbase.storage.core.obj.ScanMeta;
import com.oppo.tagbase.storage.core.util.ConstantStorageUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HbaseStorageConnector extends StorageConnector {

    @Inject
    private HbaseStorageConnectorConfig config;

    private Connection connection = null;

    private Admin admin = null;

    private ExecutorService hbasePool = null;

    @Override
    protected void initConnector() {
        log.info("init hbase Connector!");
        initHbaseConnection();
    }

    @Override
    protected void destroyConnector() {
        log.info("destroy hbase Connector!");
        closeHbaseConnection();
    }

    @Override
    protected void createTable(String tableName, int partition) throws IOException {
        List<String> familys = new ArrayList<>();
        familys.add(config.getFamily());
        createTable(config.getNameSpace(), tableName, familys, getSplitKeys(partition));
    }

    @Override
    public void deleteTable(String tableName) throws IOException {
        deleteTable(config.getNameSpace(), tableName);
    }

    @Override
    public void createRecord(String tableName, String key, ImmutableRoaringBitmap value) throws IOException {
        put(config.getNameSpace(), tableName, key, config.getFamily(), config.getQualifier(), BitmapUtil.serializeBitmap(value));

    }

    @Override
    public void createBatchRecords(String tableName, String dataPath) throws Exception {
        createTable(tableName);
        bulkLoad(tableName, dataPath);
    }

    @Override
    protected void createQuery(ScanMeta scanMeta, OperatorBuffer buffer) throws IOException {

        String tableName = scanMeta.getSlicePartition().getHbaseTable();
        String dayNumValue = scanMeta.getSlicePartition().getDate();
        FilterList rowFilterList = new FilterList();//构造scan filter
        Map<Integer,Integer> indexMap = new HashMap<>();//维度索引(index,returnIndex)
        String scanRegexStr = createRegexStr(scanMeta, indexMap);
        System.out.println("scan regex:" + scanRegexStr);
        Filter regexFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(scanRegexStr));
        rowFilterList.addFilter(regexFilter);

        scan(config.getNameSpace(), tableName, config.getFamily(), config.getQualifier(), config.getRowkeyDelimiter(), rowFilterList, indexMap, dayNumValue, buffer);
    }


    private String createRegexStr(ScanMeta scanMeta, Map<Integer,Integer> indexMap){

        int partition = scanMeta.getSlicePartition().getPattition();
        int index = 0;
        StringBuilder builder = new StringBuilder();
        String regexStart = ConstantStorageUtil.REGEX_START_STR;
        String regexEnd = ConstantStorageUtil.REGEX_END_STR;
        String regexAny = ConstantStorageUtil.REGEX_ANY_STR;
        builder.append(regexStart + partition + config.getRowkeyDelimiter());
        for(FilterMeta meta : scanMeta.getFilterMetaList()){
            if(meta.getReturnIndex() != ConstantStorageUtil.FLAG_NO_NEED_RETURN_DIM){
                indexMap.put(index,meta.getReturnIndex());
            }
            if(index > 0 ) {
                if (meta.getValues() == null) {
                    if (!builder.toString().endsWith(regexAny)) {
                        builder.append(regexAny);
                    }
                } else {
                    if (meta.getValues().size() > 0) {
                        builder.append("(");
                        int k = 1;
                        for (String value : meta.getValues()) {
                            builder.append(value);
                            if(k != meta.getValues().size()){
                                builder.append("|");
                            }
                            k++;
                        }
                        builder.append(")");
                        if (index != (scanMeta.getFilterMetaList().size()-1)) {
                            builder.append(config.getRowkeyDelimiter());
                        }
                    }
                }
            }
            index++;
        }
        if(!builder.toString().endsWith(regexAny)){
            builder.append(regexEnd);
        }
        return builder.toString();
    }

    private void initHbaseConnection() {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.property.clientPort", config.getZkPort());
            conf.set("hbase.zookeeper.quorum", config.getZkQuorum());
            conf.set("hbase.rootdir", config.getRootDir());
            synchronized (HbaseStorageConnector.class) {
                if (hbasePool == null) {
                    int maxThreads = 10;
                    int coreThreads = 5;
                    long keepAliveTime = 300;
                    LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
                    hbasePool = new ThreadPoolExecutor(coreThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS, workQueue);
                }
            }
            connection = ConnectionFactory.createConnection(conf, hbasePool);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void closeHbaseConnection() {
        try {
            if (connection != null) {
                connection.close();
            }
            if (admin != null) {
                admin.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        hbasePool.shutdownNow();
        try {
            if (hbasePool != null && !hbasePool.awaitTermination(10, TimeUnit.SECONDS)) {
                hbasePool.shutdownNow();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private byte[][] getSplitKeys(int partition){
        if(partition <= 1){
            return null;
        }
        byte[][] splitKeys = new byte[partition-1][];
        for(int i=1; i<=partition; i++){
            splitKeys[i] = Bytes.toBytes((i+1)+"");
        }
        return splitKeys;
    }

    private void createTable(String nameSpace, String tableName, List<String> familys, byte[][] splitKeys) throws IOException {

        createNamespaceIfExist(nameSpace);
        TableName tName = TableName.valueOf(nameSpace, tableName);
        if (!admin.tableExists(tName)) {
            HTableDescriptor tableDesc = new HTableDescriptor(tName);
            for(String family : familys) {
                HColumnDescriptor colDesc = new HColumnDescriptor(family.getBytes());
                tableDesc.addFamily(colDesc);
                colDesc.setMaxVersions(1);
                colDesc.setBloomFilterType(BloomType.ROW);
            }
            admin.createTable(tableDesc, splitKeys);
        }
    }

    private void createNamespaceIfExist(String nameSpace) throws IOException {

        for(NamespaceDescriptor space : admin.listNamespaceDescriptors()){
            if(space.getName().equals(nameSpace)){
                return;
            }
        }
        admin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
    }


    private void deleteTable(String nameSpace, String tableName) throws IOException {

        TableName tName = TableName.valueOf(nameSpace, tableName);
        if (admin.tableExists(tName)) {
            admin.disableTable(tName);
            admin.deleteTable(tName);
        }
    }

    public void put(String nameSpace, String tableName, String rowKey, String family, String qualifier, byte[] value) throws IOException {

        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), value);
        table.put(put);
        table.close();
    }

    public String get(String nameSpace, String tableName, String rowKey, String family, String qualifier) throws IOException {

        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(family.getBytes(), qualifier.getBytes());
        Result rs = table.get(get);
        Cell cell = rs.getColumnLatestCell(family.getBytes(), qualifier.getBytes());
        if (cell != null) {
            return Bytes.toString(CellUtil.cloneValue(cell));
        }
        table.close();
        return null;
    }

    private void bulkLoad(String tableName, String dataPath) throws Exception {

        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(connection.getConfiguration());
        TableName table = TableName.valueOf(tableName);
        loader.doBulkLoad(new Path(dataPath),admin,connection.getTable(table),connection.getRegionLocator(table));
    }


    public void scan(String nameSpace, String tableName, String family, String qualifier, String delimiter, FilterList filterList, Map<Integer,Integer> indexMap, String dayNumValue, OperatorBuffer buffer) {

        ResultScanner scanner = null;
        try {
            Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
            Scan scan = new Scan();
            scan.setCaching(100);
            scan.setMaxResultSize(5 * 1024 * 1024);
            scan.setFilter(filterList);
            scan.addColumn(family.getBytes(), qualifier.getBytes());
            scanner = table.getScanner(scan);
            if (scanner != null) {
                Iterator<Result> iterator = scanner.iterator();
                while (iterator.hasNext()) {
                    for (Cell cell : iterator.next().rawCells()) {
                        //切分rowkey,shardnum_app_event_version
                        String[] dimValueArr = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()).split(delimiter);

                        //List<String> dimValues = new ArrayList<>();
                        byte[][] dimValues = null;
                        if(indexMap.size() > 0 ) {
                            dimValues = new byte[indexMap.size()][];
                            for (int index : indexMap.keySet()) {
                                if(index == 0){//如果需要返回daynum
                                    dimValues[indexMap.get(index)-1] = dayNumValue.getBytes();
                                }else {
                                    dimValues[indexMap.get(index)-1] = dimValueArr[index].getBytes();
                                }
                            }
                        }
                        ImmutableRoaringBitmap bitmap = BitmapUtil.deSerializeBitmap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                        TagBitmap tagBitmap = new TagBitmap(dimValues, bitmap);
                        buffer.offer(tagBitmap);

                    }
                }
            }
        }catch (IOException e){
            log.error("hbase scan error",e);
        }finally {
            buffer.offer(TagBitmap.EOF);
            if(scanner != null){
                scanner.close();
            }
        }
    }
}
