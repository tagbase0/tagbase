package com.oppo.tagbase.storage.hbase;

import com.google.common.collect.Range;
import com.oppo.tagbase.meta.obj.ColumnType;
import com.oppo.tagbase.storage.core.connector.StorageConnector;
import com.oppo.tagbase.storage.core.exception.StorageException;
import com.oppo.tagbase.storage.core.obj.OperatorBuffer;
import com.oppo.tagbase.storage.core.util.BitmapUtil;
import com.oppo.tagbase.storage.core.obj.AggregateRow;
import com.oppo.tagbase.storage.core.obj.DimContext;
import com.oppo.tagbase.storage.core.obj.StorageQueryContext;
import com.oppo.tagbase.storage.core.util.StorageConstant;
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

/**
 * Created by liangjingya on 2020/2/8.
 */
public class HbaseStorageConnector extends StorageConnector {

    @Inject
    private HbaseStorageConnectorConfig hbaseConfig;

    private Connection connection = null;

    private Admin admin = null;

    public static String REGEX_START_STR = "^";
    public static String REGEX_ANY_STR = ".+";
    public static String REGEX_END_STR = "$";

    @Override
    protected void initConnector() {
        log.info("init hbase Connector!");
        log.info(hbaseConfig.toString());
        initHbaseConnection();
    }

    @Override
    protected void destroyConnector() {
        log.info("destroy hbase Connector!");
        closeHbaseConnection();
    }

    @Override
    protected void createTable(String dbName, String tableName, int partition) throws StorageException {
        List<String> familys = new ArrayList<>();
        familys.add(hbaseConfig.getFamily());
        createHbaseTableIfNotExist(dbName, tableName, familys, getSplitKeys(partition));
    }

    @Override
    public void deleteTable(String dbName, String tableName) throws StorageException {
        deleteHbaseTable(hbaseConfig.getNameSpace(), tableName);
    }

    @Override
    public void createRecord(String dbName, String tableName, String key, ImmutableRoaringBitmap value) throws StorageException {
        try {
            byte[] metric = BitmapUtil.serializeBitmap(value);
            put(hbaseConfig.getNameSpace(), tableName, key, hbaseConfig.getFamily(), hbaseConfig.getQualifier(), metric);
        } catch (Exception e) {
            throw new StorageException("hbaseStorageConnector createRecord error",e);
        }

    }

    @Override
    public void createBatchRecords(String dbName, String tableName, String dataPath) throws StorageException {
        createTable(hbaseConfig.getNameSpace(), tableName);
        bulkLoad(tableName, dataPath);
    }

    @Override
    protected void createStorageQuery(StorageQueryContext storageQueryContext, OperatorBuffer<AggregateRow> buffer) throws StorageException {

        String tableName = storageQueryContext.getSliceSegment().getTableName();
        String dayNumValue = storageQueryContext.getSliceSegment().getSliceDate();
        //构造scan filter
        FilterList rowFilterList = new FilterList();
        //dim索引关系(index,returnIndex)
        Map<Integer,Integer> indexMap = new HashMap<>();
        String scanRegexStr = createScanRegexStr(storageQueryContext, indexMap);
        log.debug("scan filter regex string : " + scanRegexStr);
        Filter regexFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(scanRegexStr));
        rowFilterList.addFilter(regexFilter);

        scan(hbaseConfig.getNameSpace(), tableName, hbaseConfig.getFamily(), hbaseConfig.getQualifier(), hbaseConfig.getRowkeyDelimiter(), rowFilterList, indexMap, dayNumValue, buffer);
    }


    private String createScanRegexStr(StorageQueryContext storageQueryContext, Map<Integer,Integer> indexMap){

        int segmentId = storageQueryContext.getSliceSegment().getSegmentId();
        int index = 1;
        StringBuilder builder = new StringBuilder();
        builder.append(REGEX_START_STR + segmentId + hbaseConfig.getRowkeyDelimiter());
        for(DimContext meta : storageQueryContext.getDimContextList()){
            if(meta.getType() == ColumnType.DIM_COLUMN ) {
                if(meta.getDimReturnIndex() != StorageConstant.FLAG_NO_NEED_RETURN){
                    indexMap.put(index, meta.getDimReturnIndex());
                }
                if (meta.getDimValues() == null) {
                    if (!builder.toString().endsWith(REGEX_ANY_STR)) {
                        builder.append(REGEX_ANY_STR);
                    }
                } else {
                    int size = meta.getDimValues().asRanges().size();
                    if (size > 0) {
                        builder.append("(");
                        int k = 1;
                        for (Range<String> value : meta.getDimValues().asRanges()) {
                            builder.append(value.lowerEndpoint());
                            if(k != size){
                                builder.append("|");
                            }
                            k++;
                        }
                        builder.append(")");
                        builder.append(hbaseConfig.getRowkeyDelimiter());
                    }
                }
                index++;
            }else if(meta.getType() == ColumnType.SLICE_COLUMN ){
                indexMap.put(meta.getDimIndex(), meta.getDimReturnIndex());
            }
            if(index >= storageQueryContext.getDimContextList().size() &&
                    builder.toString().endsWith(hbaseConfig.getRowkeyDelimiter())){
                builder.deleteCharAt(builder.length() - 1);
            }
        }
        if(!builder.toString().endsWith(REGEX_ANY_STR)){
            builder.append(REGEX_END_STR);
        }
        return builder.toString();
    }

    private void initHbaseConnection() {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.property.clientPort", hbaseConfig.getZkPort());
            conf.set("hbase.zookeeper.quorum", hbaseConfig.getZkQuorum());
            conf.set("hbase.rootdir", hbaseConfig.getRootDir());
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            log.error("init hbase connection error", e);
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
            log.error("close hbase connection error", e);
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

    private void createHbaseTableIfNotExist(String nameSpace, String tableName, List<String> familys, byte[][] splitKeys) throws StorageException {

        try {
            createNamespaceIfNotExist(nameSpace);
            TableName tName = TableName.valueOf(nameSpace, tableName);
            if (!admin.tableExists(tName)) {
                HTableDescriptor tableDesc = new HTableDescriptor(tName);
                for (String family : familys) {
                    HColumnDescriptor colDesc = new HColumnDescriptor(family.getBytes());
                    tableDesc.addFamily(colDesc);
                    colDesc.setMaxVersions(1);
                    colDesc.setBloomFilterType(BloomType.ROW);
                }
                admin.createTable(tableDesc, splitKeys);
            }
        }catch (Exception e){
            throw new StorageException("hbaseStorageConnector createTable error",e);
        }
    }

    private void createNamespaceIfNotExist(String nameSpace) throws StorageException {

        try {
            for(NamespaceDescriptor space : admin.listNamespaceDescriptors()){
                if(space.getName().equals(nameSpace)){
                    return;
                }
            }
            admin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
        } catch (Exception e) {
            throw new StorageException("hbaseStorageConnector createNamespace error",e);
        }

    }


    private void deleteHbaseTable(String nameSpace, String tableName) throws StorageException {

        TableName tName = TableName.valueOf(nameSpace, tableName);
        try {
            if (admin.tableExists(tName)) {
                admin.disableTable(tName);
                admin.deleteTable(tName);
            }
        } catch (Exception e) {
            throw new StorageException("hbaseStorageConnector deleteHbaseTable error",e);
        }
    }

    public void put(String nameSpace, String tableName, String rowKey, String family, String qualifier, byte[] value) throws StorageException {

        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(nameSpace, tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), value);
            table.put(put);
        }catch (IOException e){
            throw new StorageException("hbaseStorageConnector put error",e);
        }finally {
            if(table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    log.error("hbase table close error", e);
                }
            }
        }

    }

    public String get(String nameSpace, String tableName, String rowKey, String family, String qualifier) throws StorageException {

        Table table = null;
        try {
            connection.getTable(TableName.valueOf(nameSpace, tableName));
            Get get = new Get(rowKey.getBytes());
            get.addColumn(family.getBytes(), qualifier.getBytes());
            Result rs = table.get(get);
            Cell cell = rs.getColumnLatestCell(family.getBytes(), qualifier.getBytes());
            if (cell != null) {
                return Bytes.toString(CellUtil.cloneValue(cell));
            }
        }catch (Exception e){
            throw new StorageException("hbaseStorageConnector get error",e);
        }finally {
            if(table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    log.error("hbase table close error", e);
                }
            }
        }
        return null;
    }

    private void bulkLoad(String tableName, String dataPath) throws StorageException {

        try {
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(connection.getConfiguration());
            TableName table = TableName.valueOf(hbaseConfig.getNameSpace(), tableName);
            loader.doBulkLoad(new Path(dataPath), admin, connection.getTable(table), connection.getRegionLocator(table));
        }catch (Exception e){
            throw new StorageException("hbaseStorageConnector bulkLoad error",e);
        }
    }


    public void scan(String nameSpace, String tableName, String family, String qualifier, String delimiter, FilterList filterList, Map<Integer,Integer> indexMap, String dayNumValue, OperatorBuffer<AggregateRow> buffer) throws StorageException {

        ResultScanner scanner = null;
        try {
            Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
            Scan scan = new Scan();
            scan.setCaching(hbaseConfig.getScanCacheSize());
            scan.setMaxResultSize(hbaseConfig.getScanMaxResultSize());
            scan.setFilter(filterList);
            scan.addColumn(family.getBytes(), qualifier.getBytes());
            scanner = table.getScanner(scan);
            if (scanner != null) {
                Iterator<Result> iterator = scanner.iterator();
                while (iterator.hasNext()) {
                    for (Cell cell : iterator.next().rawCells()) {
                        //切分rowkey
                        String[] dimValueArr = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()).split(delimiter);

                        byte[][] dimValues = null;
                        if(indexMap.size() > 0 ) {
                            dimValues = new byte[indexMap.size()][];
                            for (int index : indexMap.keySet()) {
                                if(index == StorageConstant.SLICE_COLUMN_INDEX){//如果需要返回sliceColumn
                                    dimValues[indexMap.get(index)-1] = dayNumValue.getBytes();
                                }else {
                                    dimValues[indexMap.get(index)-1] = dimValueArr[index].getBytes();
                                }
                            }
                        }
                        ImmutableRoaringBitmap bitmap = BitmapUtil.deSerializeBitmap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                        AggregateRow aggregateRow = new AggregateRow(dimValues, bitmap);
                        buffer.postData(aggregateRow);
                    }
                }
            }
            buffer.postEnd();
        }catch (Exception e){
            StorageException exception = new StorageException("hbaseStorageConnector scan error",e);
            buffer.fastFail(exception);
            throw exception;
        }finally {
            if(scanner != null){
                scanner.close();
            }
        }
    }
}
