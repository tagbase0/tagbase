package com.oppo.tagbase.query;

import com.oppo.tagbase.query.node.Query;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author huangfeng
 * @date 2020/2/26 20:55
 */
public class BasePhysicalPlanTest extends BaseAnalyzerTest {

    protected static PhysicalPlanner PHYSICAL_PLANNER;

    @BeforeClass
    public static void prepare() {
        PHYSICAL_PLANNER = new PhysicalPlanner();
    }


    @Test
    public void testPlanSingleQueryForProvince() throws IOException {

        PhysicalPlan physicalPlan = generatePhysicalPlan("province.query");
        System.out.println(physicalPlan.toString());
    }
    @Test
    public void testPlanComplexQuery() throws IOException {
        PhysicalPlan physicalPlan = generatePhysicalPlan("people_analysis.query");
        System.out.println(physicalPlan.toString());
    }


    protected PhysicalPlan generatePhysicalPlan(String queryPath) throws IOException {
        Query query = buildQueryFromFile(queryPath);
        Analysis analysis = SEMANTIC_ANALYZER.analyze(query);
        return PHYSICAL_PLANNER.plan(query, analysis);
    }


    private void assertEqualPlan(PhysicalPlan physicalPlan) {
        assertEquals("", physicalPlan.toString());
    }


}
