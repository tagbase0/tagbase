package com.oppo.tagbase.job.util;

import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Created by daikai on 2020/2/23.
 */
public class IdGeneratorTest {

    @Test
    public void nextQueryId() {

        System.out.println(new IdGenerator().nextQueryId("DictBuildJob", "yyyyMMdd"));
        System.out.println(new IdGenerator().nextQueryId("DictBuildJob"));
    }

}