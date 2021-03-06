package com.oppo.tagbase.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.query.mock.MetadataMock;
import com.oppo.tagbase.query.node.Query;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
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
        Metadata metadata = MetadataMock.mockMetadata();
        SEMANTIC_ANALYZER = new SemanticAnalyzer(metadata);
        JSON_MAPPER = new ObjectMapper();
    }


    @Test
    public void testAnalyzeSingleQueryForProvince() throws IOException {
        Query query =  buildQueryFromFile("province.query");
        assertSingleQueryAnalysis(SEMANTIC_ANALYZER.analyze(query),query,"tagbase","province",ImmutableList.<String>of(), ImmutableMap.of("province",1));
    }

    @Test
    public void testAnalyzeSinQueryForBehavior() throws IOException {
        Query query = buildQueryFromFile("behavior.query");
        assertSingleQueryAnalysis( SEMANTIC_ANALYZER.analyze(query),query,"tagbase","behavior",ImmutableList.of("app"),  ImmutableMap.of("app",2,"dayno",3,"behavior",1));
    }


    @Test
    public void testAnalyzeComplexQuery() throws IOException {
        Query query = buildQueryFromFile("people_analysis.query");
        assertComplexQueryAnalysis(SEMANTIC_ANALYZER.analyze(query),query,"[{id='1,2', columns=[province], fields=[STRING]}]");
    }



    private void assertComplexQueryAnalysis(Analysis analysis, Query query, String info) {
        assertEquals(analysis.getScope(query).getOutputMeta().values().toString(),info);
    }


    protected Query buildQueryFromFile(String filePath) throws IOException {
        return JSON_MAPPER.readValue(getResourceFile(filePath), Query.class);
    }

    private void assertSingleQueryAnalysis(Analysis analysis, Query query, String dbName, String tableName, ImmutableList<String> groupByColumns, ImmutableMap<String,Integer> filterCardinality) {

        assertEquals(dbName,analysis.getQueryDB(query));
        assertEquals(tableName,analysis.getQueryTable(query).getName());
        assertEquals(groupByColumns,analysis.getDims(query));
        Map<String,Integer> actualCardinality = analysis.getQueryFilterAnalysis(query).values().stream().collect(Collectors.toMap(item->item.getColumn().getName(),item->item.getCardinality()));
        assertEquals(filterCardinality,actualCardinality);
    }



    File getResourceFile(String fileName) {
        return new File(this.getClass().getClassLoader().getResource(fileName).getPath());

    }

}
