package com.oppo.tagbase.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.query.node.Query;
import com.oppo.tagbase.query.row.AggregateRow;
import com.oppo.tagbase.query.row.Dimensions;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * @author huangfeng
 * @date 2020/2/25 20:06
 */
public class SemanticAnalyzerTest {
    private static SemanticAnalyzer SEMANTIC_ANALYZER;

    @BeforeClass
    public static void setUp() {

        Metadata metadata = MockMetadata.mockMetadata();
        SEMANTIC_ANALYZER = new SemanticAnalyzer(metadata);

    }


    @Test
    public void testSingleQuery() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Query query = objectMapper.readValue(getResourceFile("province.query"), Query.class);

        List<AggregateRow> rows = readDataFrom("province.data");

        Analysis analysis = SEMANTIC_ANALYZER.analyze(query);
        PhysicalPlanner physicalPlanner = new PhysicalPlanner();
        PhysicalPlan physicalPlan = physicalPlanner.plan(query, analysis);

        ExecutorService testExecutor = Executors.newFixedThreadPool(3);
        QueryEngine queryExecutor = new QueryEngine(testExecutor);

        System.out.println(physicalPlan);

    }

    private List<AggregateRow> readDataFrom(String dataPath) {
        ImmutableList.Builder<AggregateRow> rowBuilder = ImmutableList.builder();
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

                if (dimStr.length != 0 && dimStr[0].equals("")) {
                    byteDims = new byte[dimStr.length][];
                    for (int n = 0; n < dimStr.length; n++) {
                        byteDims[n] = dimStr[n].getBytes();
                    }


                }
                rowBuilder.add(new AggregateRow("0", new Dimensions(byteDims), bitmap));
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

    File getResourceFile(String fileName) {
        return new File(this.getClass().getClassLoader().getResource(fileName).getPath());

    }

}
