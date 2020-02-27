package com.oppo.tagbase.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.query.mock.MockMetadata;
import com.oppo.tagbase.query.node.Query;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * @author huangfeng
 * @date 2020/2/25 20:06
 */
public class BaseAnalyzerTest {
    protected static SemanticAnalyzer SEMANTIC_ANALYZER;
    protected static ObjectMapper JSON_MAPPER;

    @BeforeClass
    public static void setUp() {
        Metadata metadata = MockMetadata.mockMetadata();
        SEMANTIC_ANALYZER = new SemanticAnalyzer(metadata);
        JSON_MAPPER = new ObjectMapper();
    }


    @Test
    public void testSingleQuery() throws IOException {

        Query query =  buildQueryFromFile("province.query");
        Analysis analysis = SEMANTIC_ANALYZER.analyze(query);

        assertSingleQueryAnalysis(analysis,query,"tagbase","province",ImmutableList.<String>of(), Arrays.asList(1));
    }





    protected Query buildQueryFromFile(String filePath) throws IOException {
        return JSON_MAPPER.readValue(getResourceFile(filePath), Query.class);
    }

    private void assertSingleQueryAnalysis(Analysis analysis, Query query, String dbName, String tableName, ImmutableList<String> groupByColumns, List<Integer> filterCardinality) {

        assertEquals(dbName,analysis.getQueryDB(query));
        assertEquals(tableName,analysis.getQueryTable(query).getName());
        assertEquals(groupByColumns,analysis.getDims(query));
        List<Integer> actualCardinality = analysis.getQueryFilterAnalysis(query).values().stream().map(item -> item.getCardinality()).collect(Collectors.toList());
        assertEquals(filterCardinality,actualCardinality);


    }



    File getResourceFile(String fileName) {
        return new File(this.getClass().getClassLoader().getResource(fileName).getPath());

    }

}
