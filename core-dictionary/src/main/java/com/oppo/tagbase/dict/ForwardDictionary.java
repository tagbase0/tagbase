package com.oppo.tagbase.dict;

import com.oppo.tagbase.dict.util.FileUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.oppo.tagbase.dict.GroupMeta.GROUP_LENGTH;

/**
 *
 * Meta length:
 *  magic : 20
 *  version : 3
 *  lastModifiedDate : 8
 *  checkSum : 8
 *  groupNum : 4
 *  totalElementNum : 8
 *
 *
 * Created by wujianchao on 2020/2/11.
 */
public final class ForwardDictionary extends AbstractDictionary {

    /**
     * Max element size approximately 2 billion
     * For Java max array length is Integer.MAX_VALUE - 8, so temporarily limit to the value.
     */
    public static final int MAX_ELEMENT = Integer.MAX_VALUE - 8;

    private File file;

    private AtomicBoolean ready = new AtomicBoolean(false);

    private ForwardDictionaryMeta meta;

    private Group[] groups;

    /**
     * first element id of group in the whole dictionary
     */
    private long[] groupFirstElementId;

    /**
     * dictionary file length
     */
    private long dictLength;

    private ForwardDictionary(File file) throws IOException {
        if(!file.exists()) {
            throw  new DictionaryException("Dictionary file not existed " + file.getAbsolutePath());
        }
        this.file = file;
        loadDictionary();
    }

    @DictionaryApi
    public static ForwardDictionary create(File file) throws IOException {
        return new ForwardDictionary(file);
    }

    private void loadDictionary() throws IOException {
        if(ready.get()) {
            throw new DictionaryException("ForwardDictionary already initialized");
        }
        this.dictLength = file.length();

        try (FileInputStream in = new FileInputStream(file);
             FileChannel channel = in.getChannel()) {

            // load meta
            byte[] metaBytes = FileUtil.read(channel, 0, meta.length());
            meta = ForwardDictionaryMeta.deserialize(metaBytes);

            //load groups
            //TODO replace Group[] with Group[][]
            groups = new Group[(int) meta.getGroupNum()];
            for(int i=0; i<meta.getGroupNum(); i++) {
                groups[i] = readGroup(channel, meta, i);
            }

            // init group first element id index
            //TODO replace [] with [][]
            groupFirstElementId = new long[(int) meta.getGroupNum()];
            if(meta.getGroupNum() > 0L) {
                groupFirstElementId[0] = groups[0].getElementNum();
                for(int i=1; i<meta.getGroupNum(); i++) {
                    groupFirstElementId[i] = groups[i].getElementNum();
                }
            }
        }


        ready.compareAndSet(false, true);
    }

    static Group readGroup(FileChannel channel, ForwardDictionaryMeta meta, long groupId) throws IOException {
        long off = groupOffset(meta, groupId);
        return Group.createGroup(FileUtil.read(channel, off, GROUP_LENGTH));
    }

    /**
     * Group offset in file.
     */
    static long groupOffset(ForwardDictionaryMeta meta, long groupId) {
        if(groupId >= meta.getGroupNum()){
            throw new DictionaryException(String.format("group id %d can not larger than groupNum %d", groupId, meta.getGroupNum()));
        }
        return meta.length() + groupId * GROUP_LENGTH;
    }

    @Override
    @DictionaryApi
    public byte[] element(long id) {
        checkId(id);

        long groupId = findGroupId(id);
        long groupOff = id - groupFirstElementId[(int) groupId];

        return groups[(int) groupId].element((int) groupOff);
    }

    private void checkId(long id) {
        if(id < 0 || id > meta.getElementNum()) {
            throw new DictionaryException(String.format("Element id should between %d and %d", 0, meta.getElementNum()));
        }
    }

    /**
     * TODO binary search fashion to
     */
    private long findGroupId(long id) {
        long groupId   = 0L;
        for(int i=0; i<meta.getElementNum(); i++) {
            if(id < groupFirstElementId[i]) {
                groupId = i - 1;
                break;
            }
            if(id == groupFirstElementId[i]) {
                groupId = i;
                break;
            }
        }
        return groupId;
    }

    @Override
    @DictionaryApi
    public long elementNum() {
        return meta.getElementNum();
    }

    @Override
    public long id(byte[] element) {
        throw new UnsupportedOperationException("Forward dictionary doesn't support search id by element");
    }
}
