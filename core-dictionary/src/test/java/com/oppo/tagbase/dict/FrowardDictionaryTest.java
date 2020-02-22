package com.oppo.tagbase.dict;

import com.oppo.tagbase.common.util.BytesUtil;
import com.oppo.tagbase.common.util.FileUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static com.oppo.tagbase.dict.ForwardDictionaryWriter.MAX_ELEMENT_LENGTH;
import static com.oppo.tagbase.dict.Group.GROUP_LENGTH;

/**
 * Created by wujianchao on 2020/2/21.
 */
public class FrowardDictionaryTest {


    @Test
    public void sanityTest() throws IOException {
        File dictFile = FileUtil.createDeleteOnExitFile("target/forward-dict-sanity-test.txt");
        ForwardDictionaryWriter writer = ForwardDictionaryWriter.createWriter(dictFile);

        List<byte[]> elementList = new ArrayList<>();
        elementList.add(BytesUtil.toUTF8Bytes("123"));
        elementList.add(BytesUtil.toUTF8Bytes("45"));
        elementList.add(BytesUtil.toUTF8Bytes("678"));
        elementList.add(BytesUtil.toUTF8Bytes("9"));

        for (byte[] element : elementList) {
            writer.add(element);
        }

        writer.complete();

        try(FileInputStream in = new FileInputStream(dictFile);
            FileChannel channel = in.getChannel()) {

            byte[] metaBytes = FileUtil.read(channel, 0, ForwardDictionaryMeta.length());
            ForwardDictionaryMeta deserializedMeta = ForwardDictionaryMeta.deserialize(metaBytes);

            Assert.assertEquals(1, deserializedMeta.getGroupNum());
            Assert.assertEquals(elementList.size(), deserializedMeta.getElementNum());
        }

        Assert.assertEquals(ForwardDictionaryMeta.length() + 1 * GROUP_LENGTH, dictFile.length());


        ForwardDictionary dict = ForwardDictionary.create(dictFile);

        for (int i = 0; i < elementList.size(); i++) {
            Assert.assertArrayEquals(elementList.get(i), dict.element(i));
        }

    }

    @Test
    public void addToExistedDictTest() throws IOException {

        File dictFile = FileUtil.createDeleteOnExitFile("target/forward-dict-add-to-existed-test.txt");
        ForwardDictionaryWriter writer = ForwardDictionaryWriter.createWriter(dictFile);


        // first batch
        List<byte[]> elementList = new ArrayList<>();
        elementList.add(BytesUtil.toUTF8Bytes("1"));
        elementList.add(BytesUtil.toUTF8Bytes("2"));
        elementList.add(BytesUtil.toUTF8Bytes("3"));
        elementList.add(BytesUtil.toUTF8Bytes("4"));

        for (byte[] element : elementList) {
            writer.add(element);
        }

        // to expand group num to 2
        for(int i=0; i<4000; i++) {
            writer.add(ElementGenerator.generate(20));
        }
        writer.complete();

        // second batch
        ForwardDictionaryWriter secondWriter = ForwardDictionaryWriter.createWriterForExistedDict(dictFile);
        for (byte[] element : elementList) {
            secondWriter.add(element);
        }
        secondWriter.complete();

        // third batch
        List<byte[]> anoElementBatch = new ArrayList<>();
        anoElementBatch.add(BytesUtil.toUTF8Bytes("a"));
        anoElementBatch.add(BytesUtil.toUTF8Bytes("b"));
        anoElementBatch.add(BytesUtil.toUTF8Bytes("c"));
        anoElementBatch.add(BytesUtil.toUTF8Bytes("d"));
        ForwardDictionaryWriter thirdWriter = ForwardDictionaryWriter.createWriterForExistedDict(dictFile);
        for (byte[] element : anoElementBatch) {
            thirdWriter.add(element);
        }
        thirdWriter.complete();

        ForwardDictionary dict = ForwardDictionary.create(dictFile);


        // assert file size
        Assert.assertEquals(ForwardDictionaryMeta.length() + 2 * GROUP_LENGTH, dictFile.length());

        // assert element num
        Assert.assertEquals(4012, dict.elementNum());

        // assert element
        Assert.assertArrayEquals(BytesUtil.toUTF8Bytes("1"), dict.element(0));
        Assert.assertArrayEquals(BytesUtil.toUTF8Bytes("4"), dict.element(3));
        Assert.assertArrayEquals(BytesUtil.toUTF8Bytes("1"), dict.element(4004));
        Assert.assertArrayEquals(BytesUtil.toUTF8Bytes("4"), dict.element(4007));
        Assert.assertArrayEquals(BytesUtil.toUTF8Bytes("a"), dict.element(4008));
        Assert.assertArrayEquals(BytesUtil.toUTF8Bytes("d"), dict.element(4011));

    }


    @Test
    public void addElementTest() throws IOException {
        File dictFile = FileUtil.createDeleteOnExitFile("target/forward-dict-add-element-test.txt");
        ForwardDictionaryWriter writer = ForwardDictionaryWriter.createWriter(dictFile);

        writer.add(BytesUtil.toUTF8Bytes("1"));
        writer.complete();
        Assert.assertTrue(true);
    }

    @Test(expected = DictionaryException.class)
    public void addNullElementTest() throws IOException {
        File dictFile = FileUtil.createDeleteOnExitFile("target/forward-dict-add-element-test.txt");
        ForwardDictionaryWriter writer = ForwardDictionaryWriter.createWriter(dictFile);

        writer.add(null);
        writer.complete();
    }

    @Test(expected = DictionaryException.class)
    public void addBlankElementTest() throws IOException {
        File dictFile = FileUtil.createDeleteOnExitFile("target/forward-dict-add-element-test.txt");
        ForwardDictionaryWriter writer = ForwardDictionaryWriter.createWriter(dictFile);

        writer.add(new byte[]{});
        writer.complete();
    }

    @Test(expected = DictionaryException.class)
    public void addTooLongElementTest() throws IOException {
        File dictFile = FileUtil.createDeleteOnExitFile("target/forward-dict-add-element-test.txt");
        ForwardDictionaryWriter writer = ForwardDictionaryWriter.createWriter(dictFile);

        writer.add(ByteBuffer.allocate(MAX_ELEMENT_LENGTH + 1).array());
        writer.complete();
    }

}
