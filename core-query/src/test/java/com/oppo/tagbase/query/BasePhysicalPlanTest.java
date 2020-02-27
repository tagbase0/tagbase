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
    public static  void prepare(){
        PHYSICAL_PLANNER = new PhysicalPlanner();
    }


    @Test
    public void test() throws IOException {

        Query query =  buildQueryFromFile("province.query");
        Analysis analysis = SEMANTIC_ANALYZER.analyze(query);

        PhysicalPlan physicalPlan = PHYSICAL_PLANNER.plan(query, analysis);

//        assertEqualPlan(physicalPlan);



    }

    private void assertEqualPlan(PhysicalPlan physicalPlan) {
        assertEquals("",physicalPlan.toString());
    }




}
