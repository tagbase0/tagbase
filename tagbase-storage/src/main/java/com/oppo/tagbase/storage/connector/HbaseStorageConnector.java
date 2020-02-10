package com.oppo.tagbase.storage.connector;

//import com.oppo.tagbase.query.node.SingleQuery;

import com.oppo.tagbase.storage.bean.BitmapBean;
import com.oppo.tagbase.storage.util.BitmapUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by liangjingya on 2020/2/8.
 */
public class HbaseStorageConnector implements StorageConnector {

    @Inject
    private HbaseStorageConnectorConfig config;

    private Connection connection = null;

    private Admin admin = null;

    private ExecutorService executor = null;

    @Override
    public void initConnector() {
        System.out.println("init hbase Connector!");
        initHbaseConnection();
    }

    @Override
    public void destroyConnector() {
        System.out.println("destroy hbase Connector!");
        closeHbaseConnection();
    }

    @Override
    public boolean createTable(String tableName) throws IOException {

        return createTable(config.getNameSpace(), tableName, config.getFamily(), null);

    }

    @Override
    public boolean deleteTable(String tableName) throws IOException {

        return deleteTable(config.getNameSpace(), tableName);
    }

    @Override
    public void addRecord(String tableName, String key, ImmutableRoaringBitmap value) throws IOException {

        put(config.getNameSpace(), tableName, key, config.getFamily(), config.getQualifier(), BitmapUtil.serializeBitmap(value));

    }

    @Override
    public void bulkLoadRecords(String dataPath) {


    }

    @Override
    public Iterator<BitmapBean> getRecords(String tableName) throws IOException {

        /*
            此处跟元数据交互、解析SingleQuery、daynum多线程处理，再细化代码
         */

        String shardPrefix = "1";
        FilterList filterList = new FilterList();
        filterList.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(shardPrefix.getBytes())));
        int[] arr = {1};
        return scan(config.getNameSpace(), tableName, config.getFamily(), config.getQualifier(), config.getRowkeyDelimiter(), filterList, arr);
    }

    private void initHbaseConnection() {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.property.clientPort", config.getZkPort());
            conf.set("hbase.zookeeper.quorum", config.getZkQuorum());
            conf.set("hbase.rootdir", config.getRootDir());
            executor = Executors.newFixedThreadPool(config.getThreadPoolSize());
            connection = ConnectionFactory.createConnection(conf, executor);
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
            executor.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean createTable(String nameSpace, String tableName, String family, byte[][] splitKeys) throws IOException {

        TableName tName = TableName.valueOf(nameSpace, tableName);
        if (admin.tableExists(tName)) {
            return false;
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tName);
            HColumnDescriptor colDesc = new HColumnDescriptor(family.getBytes());
            tableDesc.addFamily(colDesc);
            colDesc.setMaxVersions(1);
            colDesc.setBloomFilterType(BloomType.ROW);
            admin.createTable(tableDesc, splitKeys);
        }
        return true;
    }

    private boolean deleteTable(String nameSpace, String tableName) throws IOException {

        TableName tName = TableName.valueOf(nameSpace, tableName);
        if (admin.tableExists(tName)) {
            admin.disableTable(tName);
            admin.deleteTable(tName);
        }
        return true;
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

    public Iterator<BitmapBean> scan(String nameSpace, String tableName, String family, String qualifier, String delimiter, FilterList filterList, int[] dimIndex) throws IOException {

        LinkedList<BitmapBean> bitmapList = new LinkedList<>();
        Table table = connection.getTable(TableName.valueOf(nameSpace, tableName));
        Scan scan = new Scan();
        scan.setCaching(100);
        scan.setFilter(filterList);
        scan.addColumn(family.getBytes(), qualifier.getBytes());
        ResultScanner scanner = table.getScanner(scan);
        if (scanner != null) {
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    //切分rowkey,shardnum_app_event_version
                    String[] dimValues = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()).split(delimiter);
                    StringBuilder dimBuilder = new StringBuilder();
                    for (int index : dimIndex) {
                        if (index < dimValues.length) {
                            dimBuilder.append(dimValues[index]);
                        }
                    }
                    ImmutableRoaringBitmap bitmap = BitmapUtil.deSerializeBitmap(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    bitmapList.add(new BitmapBean(dimBuilder.toString(), bitmap));

                }
            }
        }
        scanner.close();
        return bitmapList.iterator();

    }
}
