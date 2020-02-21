package com.oppo.tagbase.dict.benchmark;

import com.oppo.tagbase.dict.ElementGenerator;
import com.oppo.tagbase.dict.ForwardDictionary;
import com.oppo.tagbase.dict.ForwardDictionaryWriter;
import com.oppo.tagbase.dict.util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Please set at least 4GB heap RAM for benchmark testing.
 *
 * ASUS Laptop Intel Core i7-4720HQ Test result (time are all millisecond):
 *
 * Benchmark Test : dictionary[100000000] elements - search[1000000] times
 * generate dictionary time : 44585
 * load dictionary time : 4030
 * search dictionary time : 11316
 * every search time : 0
 *
 * Created by wujianchao on 2020/2/21.
 */
public class ForwardDictionaryBenchmark {

    public static void main(String[] args) throws IOException {

//        addElementBenchmarkTest(100_000_000);
        benchmarkTest(100_000_000, 1_000_000);

    }

    @Deprecated
    private static void addElementBenchmarkTest(int count) throws IOException {

        File dictFile = FileUtil.createDeleteOnExitFile("target/forward-dict-add-element-benchmark.dict");
        long startTime = System.currentTimeMillis();

        ForwardDictionaryWriter writer = ForwardDictionaryWriter.createWriter(dictFile);

        for(int i=0; i<count; i++) {
            writer.add(ElementGenerator.generate(20));
        }

        writer.complete();

        long stopTime = System.currentTimeMillis();

        System.out.println(String.format("add %d element time take %d", count, (startTime - stopTime)));
        System.out.println("dictionary file size : " + dictFile.length());
    }



    private static void benchmarkTest(int count, int searchTimes) throws IOException {
        File dictFile = FileUtil.createDeleteOnExitFile("target/forward-dict-benchmark.dict");
        ForwardDictionaryWriter writer = ForwardDictionaryWriter.createWriter(dictFile);

        long generateDictTime = System.currentTimeMillis();

        for(int i=0; i<count; i++) {
            writer.add(ElementGenerator.generate(20));
        }

        writer.complete();

        long loadDictTime = System.currentTimeMillis();
        ForwardDictionary dict = ForwardDictionary.create(dictFile);

        long searchTime = System.currentTimeMillis();
        searchTimes = searchTimes < count ? searchTimes : count;

        for(int i=0; i<searchTimes; i++) {
            dict.element(ThreadLocalRandom.current().nextInt(count));
        }

        long doneTime = System.currentTimeMillis();

        System.out.println(String.format("Benchmark Test : dictionary[%d] elements - search[%d] times", count, searchTimes));
        System.out.println("generate dictionary time : " + (loadDictTime - generateDictTime));
        System.out.println("load dictionary time : " + (searchTime - loadDictTime));
        System.out.println("search dictionary time : " + (doneTime - searchTime));
        System.out.println("every search time : " + (doneTime - searchTime)/searchTimes);

    }

}
