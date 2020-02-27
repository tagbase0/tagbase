package com.oppo.tagbase.storage.hbase;

import com.oppo.tagbase.meta.obj.ColumnType;
import com.oppo.tagbase.storage.core.connector.StorageConnector;
import com.oppo.tagbase.storage.core.exception.StorageErrorCode;
import com.oppo.tagbase.storage.core.exception.StorageException;
import com.oppo.tagbase.storage.core.obj.OperatorBuffer;
import com.oppo.tagbase.storage.core.util.BitmapUtil;
import com.oppo.tagbase.storage.core.obj.RawRow;
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
import java.util.stream.Collectors;

/**
 * Created by liangjingya on 2020/2/8.
 */
public class HbaseStorageConnector extends StorageConnector {

    @Inject
    private HbaseStorageConnectorConfig hbaseConfig;

    private Connection connection = null;

    private Admin admin = null;

    /**
     * init hbase connector
     */
    @Override
    protected void initConnector() throws StorageException  {
        log.info("init hbase Connector!");
        log.info(hbaseConfig.toString());
        initHbaseConnection();
    }

    /**
     * destroy hbase connector
     */
    @Override
    protected void destroyConnector() {
        log.info("destroy hbase Connector!");
        closeHbaseConnection();
    }

    /**
     * create hbase table
     */
    @Override
    protected void createTable(String dbName, String tableName, int partition) throws StorageException {
        List<String> familys = new ArrayList<>();
        familys.add(hbaseConfig.getFamily());
        createHbaseTableIfNotExist(dbName, tableName, familys, getSplitKeys(partition));
    }

    /**
     * delete hbase table
     */
    @Override
    public void deleteTable(String dbName, String tableName) throws StorageException {
        deleteHbaseTable(hbaseConfig.getNameSpace(), tableName);
    }

    /**
     * hbase put data
     */
    @Override
    public void createRecord(String dbName, String tableName, String key, ImmutableRoaringBitmap value) throws StorageException {
        try {
            byte[] metric = BitmapUtil.serializeBitmap(value);
            put(hbaseConfig.getNameSpace(), tableName, key, hbaseConfig.getFamily(), hbaseConfig.getQualifier(), metric);
        } catch (Exception e) {
            throw new StorageException(StorageErrorCode.STORAGE_INSERT_ERROR, e, "hbaseStorageConnector createRecord error");
        }

    }

    /**
     * hbase bulkload data
     */
    @Override
    public void createBatchRecords(String dbName, String tableName, String dataPath) throws StorageException {
        createTable(hbaseConfig.getNameSpace(), tableName);
        bulkLoad(tableName, dataPath);
    }

    /**
     * hbase query data
     */
    @Override
    protected void createStorageQuery(StorageQueryContext storageQueryContext, OperatorBuffer<RawRow> buffer) throws StorageException {

        String tableName = storageQueryContext.getSliceSegment().getTableName();
        String dayNumValue = storageQueryContext.getSliceSegment().getSliceDate();
        int index = 1;
        Map<Integer,Integer> indexMap = new HashMap<>();
        boolean equalComparatorFlag = true;
        //遍历出所有的rowkey组合, 封装成list
        List<String> rowkeyPrefixList = new ArrayList(){{add(String.valueOf(storageQueryContext.getSliceSegment().getSegmentId()));}};
        for(DimContext column : storageQueryContext.getDimContextList()){
            if(column.getType() == ColumnType.DIM_COLUMN){
                if(column.getDimReturnIndex() != StorageConstant.FLAG_NO_NEED_RETURN){
                    indexMap.put(index, column.getDimReturnIndex());
                    index++;
                }
                if(column.getDimValues() != null) {
                    List<String> finalRowkeyPrefixList = rowkeyPrefixList;
                    List<String> newRowkeyPrefixList = column.getDimValues().asRanges().stream()
                            .flatMap(value -> {
                                List<String> tmpRowkeyPrefixList = new ArrayList<>();
                                finalRowkeyPrefixList.stream()
                                        .forEach(oldRowkey -> {
                                            tmpRowkeyPrefixList.add(oldRowkey + hbaseConfig.getRowkeyDelimiter() + value.lowerEndpoint());
                                        });
                                return tmpRowkeyPrefixList.stream();
                            }).collect(Collectors.toList());
                    rowkeyPrefixList.clear();
                    rowkeyPrefixList = newRowkeyPrefixList;
                }else {
                    equalComparatorFlag = false;
                }
            }else if(column.getType() == ColumnType.SLICE_COLUMN){
                if(column.getDimReturnIndex() != StorageConstant.FLAG_NO_NEED_RETURN){
                    indexMap.put(column.getDimIndex(), column.getDimReturnIndex());
                }
            }
        }

        log.debug("rowkey prefix list {}", rowkeyPrefixList);
        scan(hbaseConfig.getNameSpace(), tableName, hbaseConfig.getFamily(), hbaseConfig.getQualifier(), hbaseConfig.getRowkeyDelimiter(), equalComparatorFlag, rowkeyPrefixList, indexMap, dayNumValue, buffer);

    }


    private void initHbaseConnection() throws StorageException {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.property.clientPort", hbaseConfig.getZkPort());
            conf.set("hbase.zookeeper.quorum", hbaseConfig.getZkQuorum());
            conf.set("hbase.rootdir", hbaseConfig.getRootDir());
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            throw new StorageException(StorageErrorCode.INIT_STORAGE_ERROR, e, "init hbase connection error");
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

    /**
     * create region split key
     */
    private byte[][] getSplitKeys(int partition){
        if(partition <= 1){
            return null;
        }
        byte[][] splitKeys = new byte[partition-1][];
        for(int i=1; i<=partition; i++){
            splitKeys[i] = Bytes.toBytes((i + 1) + "");
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
        }catch (IOException e){
            throw new StorageException(StorageErrorCode.STORAGE_TABLE_ERROR, e, "hbaseStorageConnector createTable error");
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
        } catch (IOException e) {
            throw new StorageException(StorageErrorCode.STORAGE_TABLE_ERROR, e, "hbaseStorageConnector createNamespace error",e);
        }

    }


    private void deleteHbaseTable(String nameSpace, String tableName) throws StorageException {

        TableName tName = TableName.valueOf(nameSpace, tableName);
        try {
            if (admin.tableExists(tName)) {
                admin.disableTable(tName);
                admin.deleteTable(tName);
            }
        } catch (IOException e) {
            throw new StorageException(StorageErrorCode.STORAGE_TABLE_ERROR, e, "hbaseStorageConnector deleteHbaseTable error",e);
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
            throw new StorageException(StorageErrorCode.STORAGE_INSERT_ERROR, e, "hbaseStorageConnector put error");
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
            throw new StorageException(StorageErrorCode.STORAGE_QUERY_ERROR, e, "hbaseStorageConnector get error");
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
            throw new StorageException(StorageErrorCode.STORAGE_INSERT_ERROR, e, "hbaseStorageConnector bulkLoad error");
        }
    }


    public void scan(String nameSpace, String tableName, String family, String qualifier, String delimiter, boolean equalComparatorFlag, List<String> rowkeyPrefixList, Map<Integer,Integer> indexMap, String dayNumValue, OperatorBuffer<RawRow> buffer) throws StorageException {

        ResultScanner scanner = null;
        try {
            Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
            Scan scan = new Scan();
            //设置缓冲
            scan.setCaching(hbaseConfig.getScanCacheSize());
            scan.setMaxResultSize(hbaseConfig.getScanMaxResultSize());

            for(String rowkeyPrefix : rowkeyPrefixList) {
                //设置startkey和endkey
                scan.setRowPrefixFilter(rowkeyPrefix.getBytes());
                //如果是等值查找，再设置一个BinaryComparator
                if (equalComparatorFlag) {
                    Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(rowkeyPrefix.getBytes()));
                    scan.setFilter(rowFilter);
                }
                scan.addColumn(family.getBytes(), qualifier.getBytes());
                scanner = table.getScanner(scan);
                if (scanner != null) {
                    Iterator<Result> iterator = scanner.iterator();
                    while (iterator.hasNext()) {
                        for (Cell cell : iterator.next().rawCells()) {
                            //切分rowkey
                            String[] dimValueArr = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()).split(delimiter);

                            byte[][] dimValues = null;
                            if (indexMap.size() > 0) {
                                dimValues = new byte[indexMap.size()][];
                                for (int index : indexMap.keySet()) {
                                    if (index == StorageConstant.SLICE_COLUMN_INDEX) {//如果需要返回sliceColumn
                                        dimValues[indexMap.get(index) - 1] = dayNumValue.getBytes();
                                    } else {
                                        dimValues[indexMap.get(index) - 1] = dimValueArr[index].getBytes();
                                    }
                                }
                            }
                            ImmutableRoaringBitmap bitmap = BitmapUtil.deSerializeBitmap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                            RawRow rawRow = new RawRow(dimValues, bitmap);
                            buffer.postData(rawRow);
                        }
                    }
                }
            }
            buffer.postEnd();
        }catch (Exception e){
            StorageException exception = new StorageException(StorageErrorCode.STORAGE_QUERY_ERROR, e, "hbaseStorageConnector scan error");
            buffer.fastFail(exception);
            throw exception;
        }finally {
            if(scanner != null){
                scanner.close();
            }
        }
    }
}
