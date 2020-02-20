package com.oppo.tagbase.dict;

import com.oppo.tagbase.dict.util.BytesUtil;
import com.oppo.tagbase.dict.util.FileUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.oppo.tagbase.dict.ForwardDictionary.MAX_ELEMENT;
import static com.oppo.tagbase.dict.ForwardDictionary.groupOffset;
import static com.oppo.tagbase.dict.GroupMeta.GROUP_LENGTH;
import static com.oppo.tagbase.dict.GroupWriter.GROUP_NO_ENOUGH_SPACE;

/**
 * Created by wujianchao on 2020/2/14.
 */
public final class ForwardDictionaryWriter {

    public static final int ELEMENT_IS_OK =  0;
    public static final int ELEMENT_IS_BLANK =  -1;
    public static final int ELEMENT_TOO_LONG =  -2;

    public static final int MAX_ELEMENT_LENGTH =  64;

    public static final int NOT_EXISTED = -1;

    private AtomicBoolean completed = new AtomicBoolean(false);

    private File file;

    private ForwardDictionaryMeta meta;

    private GroupWriter groupWriter;

    private long nexElementId = 0;
    private long currentGroupId = NOT_EXISTED;


    private ForwardDictionaryWriter(File file) {
        this.file = file;
    }

    /**
     * create a writer for a new forward dictionary.
     */
    @DictionaryApi
    public static ForwardDictionaryWriter createWriter(File file) throws IOException {
        ForwardDictionaryWriter writer = new ForwardDictionaryWriter(file);
        writer.initBlankDictWriter();
        return writer;
    }

    /**
     * create a writer for an existed dictionary.
     */
    @DictionaryApi
    public static ForwardDictionaryWriter createWriterForExistedDict(File file) throws IOException {
        ForwardDictionaryWriter writer = new ForwardDictionaryWriter(file);
        writer.loadExistedDict();
        return writer;
    }


    private void initBlankDictWriter() {
        this.meta = new ForwardDictionaryMeta();
    }

    private void loadExistedDict() throws IOException {

        try (FileInputStream in = new FileInputStream(file);
             FileChannel channel = in.getChannel()) {

            //read meta
            byte[] metaBytes = FileUtil.read(channel, 0, meta.length());

            //deserialize meta
            meta = ForwardDictionaryMeta.deserialize(metaBytes);

            // load currentGroup
            if (meta.getGroupNum() != 0) {
                readLastGroup(channel);
            }

            // set nexElementId
            nexElementId = meta.getElementNum();
        }
    }

    private void readLastGroup(FileChannel channel) throws IOException {
        long off = groupOffset(meta, meta.getGroupNum() -1);
        Group group = Group.createGroup(FileUtil.read(channel, off, GROUP_LENGTH));
        groupWriter = new GroupWriter(group);
    }


    /**
     * add an element into the dictionary
     *
     * @return element sequence in the dict
     */
    @DictionaryApi
    public long add(byte[] element) throws IOException {

        checkAddingCondition();

        if(checkElement(element) != ELEMENT_IS_OK) {
            throw new DictionaryException("illegal element " + BytesUtil.toUTF8String(element));
        }

        if(groupWriter == null) {
            addGroup();
        }

        int idInGroup = groupWriter.add(element);

        if(GROUP_NO_ENOUGH_SPACE == idInGroup) {

            // flush current group
            flushCurrentGroup();

            // and create new group and groupWriter
            addGroup();
        }

        groupWriter.add(element);

        if(GROUP_NO_ENOUGH_SPACE == idInGroup){
            throw new DictionaryException("can not add element " + BytesUtil.toUTF8String(element));
        }

        return ++nexElementId;
    }

    private void checkAddingCondition() {
        if(completed.get()) {
            throw new DictionaryException("Adding element failed, for writer has already closed");
        }
        if(nexElementId == MAX_ELEMENT){
            throw new DictionaryException("ForwardDictionary element length is at most " + MAX_ELEMENT);
        }
    }

    private void addGroup() {
        Group currentGroup = Group.createBlankGroup();
        groupWriter = new GroupWriter(currentGroup);
        currentGroupId ++;
    }


    private static int checkElement(byte[] element) {
        if(element == null || element.length == 0) {
            return ELEMENT_IS_BLANK;
        }

        if(element.length > MAX_ELEMENT_LENGTH) {
            return ELEMENT_TOO_LONG;
        }

        return ELEMENT_IS_OK;

    }

    /**
     * Flush meta and elements into a dictionary.
     */
    @DictionaryApi
    public void complete() throws IOException {

        if(!completed.compareAndSet(false, true)) {
            throw new DictionaryException("The writer has already completed");
        }

        flushMeta();
        flushCurrentGroup();
    }

    private void flushMeta() throws IOException {

        try(FileOutputStream out = new FileOutputStream(file)) {

            // 1. meta
            meta.setGroupNum(currentGroupId + 1);
            meta.setElementNum(nexElementId);
            meta.setLastModifiedDate(System.currentTimeMillis());

            FileUtil.write(out, 0, meta.serialize());
        }
    }

    private void flushCurrentGroup() throws IOException {
        try(FileOutputStream out = new FileOutputStream(file)) {

            long currentGroupOffset = meta.length() + GROUP_LENGTH * currentGroupId;
            FileUtil.write(out, currentGroupOffset, groupWriter.serialize());
        }
    }


}
