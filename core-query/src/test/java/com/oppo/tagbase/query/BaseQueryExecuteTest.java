package com.oppo.tagbase.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.oppo.tagbase.storage.core.connector.StorageConnector;
import com.oppo.tagbase.storage.core.obj.Dimensions;
import com.oppo.tagbase.storage.core.obj.OperatorBuffer;
import com.oppo.tagbase.storage.core.obj.QueryHandler;
import com.oppo.tagbase.storage.core.obj.RawRow;
import org.easymock.EasyMock;
import org.easymock.LogicalOperator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * @author huangfeng
 * @date 2020/2/26 21:42
 */
public class BaseQueryExecuteTest extends BasePhysicalPlanTest {

    private static QueryEngine QUERY_ENGINE;

    @BeforeClass
    public static void prepareForExecute() {
        QUERY_ENGINE = new QueryEngine(Executors.newFixedThreadPool(1));
    }


    @Test
    public void testExecuteSingQueryForBehavior() throws IOException {
        Object result = getQueryResult("behavior.query", "behavior.data");
        assertEquals("[{app=wechat, metric=6}, {app=qq, metric=4}]",result.toString());
    }


    @Test
    public void testSingQueryResult() throws IOException {
        Object result = getQueryResult("province.query", "province.data");
        assertEquals("[{metric=5}]",result.toString());
    }


    //TODO compare List<Map<String,Object>>> ???
    @Test
    public void testExecuteComplexQuery() throws IOException {
        Object result = getQueryResult("people_analysis.query", "people_analysis.data");
        assertEquals("[{province=beijing, metric=2}, {province=shanghai, metric=1}]",result.toString());
    }



    private Object getQueryResult(String queryPath, String dataPath) throws IOException {
        Map<String, List<RawRow>> dataSet = readDataSetFrom(dataPath);
        StorageConnector mock = createConnectorMock(dataSet);
        PHYSICAL_PLANNER.setStorageConnector(mock);
        PhysicalPlan physicalPlan = generatePhysicalPlan(queryPath);
        QUERY_ENGINE.execute(physicalPlan);
        return physicalPlan.getResult();
    }

    private StorageConnector createConnectorMock(Map<String, List<RawRow>> dataSet) {
        StorageConnector mock = EasyMock.createMock(StorageConnector.class);
        for (String table : dataSet.keySet()) {
            OperatorBuffer buffer = new OperatorBuffer(1);
            dataSet.get(table).forEach(buffer::postData);
            buffer.postEnd();

            EasyMock.expect(mock.createQuery(EasyMock.cmp(new QueryHandler(null, table, null, null, null, null), (a, b) -> {
                        if (a.getTableName().equals(b.getTableName())) {
                            return 0;
                        }
                        return -1;
                    }, LogicalOperator.EQUAL
            ))).andReturn(buffer);


        }
        EasyMock.replay(mock);
        return mock;
    }


    private Map<String, List<RawRow>> readDataSetFrom(String dataPath) {
        ImmutableMap.Builder<String, List<RawRow>> dataSetBuilder = ImmutableMap.builder();

        try {
            List<String> lines = Files.readLines(getResourceFile(dataPath), Charset.defaultCharset());
            int currentLine = 0;
            while (currentLine < lines.size()) {

                String table = parseTable(lines.get(currentLine));
                currentLine++;

                ImmutableList.Builder<RawRow> rowBuilder = ImmutableList.builder();
                while (currentLine < lines.size() && isNotHead(lines.get(currentLine))) {
                    rowBuilder.add(parseRow(lines.get(currentLine)));
                    currentLine++;
                }
                dataSetBuilder.put(table, rowBuilder.build());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return dataSetBuilder.build();
    }

    private RawRow parseRow(String line) {

        String dims = line.split(":")[0];
        String ids = line.split(":")[1];

        List<Integer> idList = Lists.newArrayList(ids.split(",")).stream().map(item -> Integer.parseInt(item)).collect(Collectors.toList());
        int[] idArray = new int[idList.size()];
        for (int n = 0; n < idList.size(); n++) {
            idArray[n] = idList.get(n);
        }
        ImmutableRoaringBitmap bitmap = changeToImmutable(ImmutableRoaringBitmap.bitmapOf(idArray));

        bitmap = bitmap.toMutableRoaringBitmap();

        String[] dimStr = dims.split(",");

        byte[][] byteDims = null;

        if (dimStr.length != 0 && !dimStr[0].equals("")) {
            byteDims = new byte[dimStr.length][];
            for (int n = 0; n < dimStr.length; n++) {
                byteDims[n] = dimStr[n].getBytes();
            }


        }
        return new RawRow(new Dimensions(byteDims), bitmap);
    }

    private boolean isNotHead(String line) {
        return !line.startsWith("##");
    }

    private String parseTable(String line) {
        return line.substring(2);
    }

    private ImmutableRoaringBitmap changeToImmutable(ImmutableRoaringBitmap bitmap) {
        ByteBuffer serializeBitmap = ByteBuffer.wrap(new byte[bitmap.serializedSizeInBytes()]);
        bitmap.serialize(serializeBitmap);
        serializeBitmap.flip();
        return new ImmutableRoaringBitmap(serializeBitmap);
    }

}
