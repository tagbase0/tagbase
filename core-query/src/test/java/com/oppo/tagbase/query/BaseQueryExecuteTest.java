package com.oppo.tagbase.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.oppo.tagbase.query.mock.StorageConnectorMock;
import com.oppo.tagbase.query.node.Query;
import com.oppo.tagbase.storage.core.obj.Dimensions;
import com.oppo.tagbase.storage.core.obj.RawRow;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

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
        Query query = buildQueryFromFile("behavior.query");
        Analysis analysis = SEMANTIC_ANALYZER.analyze(query);
        List<RawRow> rows = readDataFrom("behavior.data");


        PHYSICAL_PLANNER.setStorageConnector(new StorageConnectorMock(rows));

        PhysicalPlan physicalPlan = PHYSICAL_PLANNER.plan(query, analysis);


        QUERY_ENGINE.execute(physicalPlan);

        System.out.println(physicalPlan.getResult());


    }

    @Test
    public void testSingQueryResult() throws IOException {

        Query query = buildQueryFromFile("province.query");
        Analysis analysis = SEMANTIC_ANALYZER.analyze(query);

        List<RawRow> rows = readDataFrom("province.data");

        PHYSICAL_PLANNER.setStorageConnector(new StorageConnectorMock(rows));

        PhysicalPlan physicalPlan = PHYSICAL_PLANNER.plan(query, analysis);



        QUERY_ENGINE.execute(physicalPlan);

        System.out.println(physicalPlan.getResult());


    }

    @Test
    public void testExecuteComplexQuery() throws IOException {
        Query query =  buildQueryFromFile("people_analysis.query");
        Analysis analysis = SEMANTIC_ANALYZER.analyze(query);


        List<RawRow> rows = readDataFrom("people_analysis.data");
        PHYSICAL_PLANNER.setStorageConnector(new StorageConnectorMock(rows));

        PhysicalPlan physicalPlan = PHYSICAL_PLANNER.plan(query, analysis);

        System.out.println(physicalPlan);
        QUERY_ENGINE.execute(physicalPlan);
        System.out.println(physicalPlan.getResult());

    }

    private List<RawRow> readDataFrom(String dataPath) {
        ImmutableList.Builder<RawRow> rowBuilder = ImmutableList.builder();
        try {
            List<String> lines = Files.readLines(getResourceFile(dataPath), Charset.defaultCharset());

            for (String line : lines) {
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
                rowBuilder.add(new RawRow(new Dimensions(byteDims), bitmap));
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
        return rowBuilder.build();

    }

    private ImmutableRoaringBitmap changeToImmutable(ImmutableRoaringBitmap bitmap) {
        ByteBuffer serializeBitmap = ByteBuffer.wrap(new byte[bitmap.serializedSizeInBytes()]);
        bitmap.serialize(serializeBitmap);
        serializeBitmap.flip();
        return new ImmutableRoaringBitmap(serializeBitmap);
    }

}
