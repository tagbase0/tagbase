package com.oppo.tagbase.dict;

import com.oppo.tagbase.common.util.FileUtil;
import com.oppo.tagbase.common.util.Preconditions;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.oppo.tagbase.dict.ForwardDictionaryMeta.length;
import static com.oppo.tagbase.dict.ForwardDictionaryWriter.NOT_EXISTED;
import static com.oppo.tagbase.dict.Group.GROUP_LENGTH;


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
        Preconditions.check(!file.exists(),
                "Dictionary file not existed " + file.getAbsolutePath());
        this.file = file;
        loadDictionary();
    }

    @DictionaryApi
    public static ForwardDictionary create(File file) throws IOException {
        return new ForwardDictionary(file);
    }

    private void loadDictionary() throws IOException {
        Preconditions.check(ready.get(), "ForwardDictionary already initialized");
        this.dictLength = file.length();

        try (FileInputStream in = new FileInputStream(file);
             FileChannel channel = in.getChannel()) {

            // load meta
            byte[] metaBytes = FileUtil.read(channel, 0, length());
            meta = ForwardDictionaryMeta.deserialize(metaBytes);

            //load groups
            //TODO replace Group[] with Group[][]
            groups = new Group[(int) meta.getGroupNum()];
            for(int i=0; i<meta.getGroupNum(); i++) {
                groups[i] = readGroup(channel, meta, i);
            }

            // init group first element id index
            //TODO replace [] with [][]
            // Because [] index can not large than Integer.MA_VALUE,
            // so the dict file size cannot larger than 64kb * Integer.MA_VALUE
            groupFirstElementId = new long[(int) meta.getGroupNum()];
            if(meta.getGroupNum() > 0L) {
                groupFirstElementId[0] = 0;
                for(int i=1; i<meta.getGroupNum(); i++) {
                    groupFirstElementId[i] = groupFirstElementId[i-1] + groups[i-1].getElementNum();
                }
            }

            // check dict
            checkSum();
            checkConsistency();
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
        Preconditions.check(groupId >= meta.getGroupNum(),
                String.format("group id %d can not larger than groupNum %d", groupId, meta.getGroupNum()));
        return length() + groupId * GROUP_LENGTH;
    }

    @Override
    @DictionaryApi
    public byte[] element(long id) {
        checkId(id);

        long groupId = findGroupId(id);
        long idInGroup = id - groupFirstElementId[(int) groupId];

        return groups[(int) groupId].element((int) idInGroup);
    }

    /**
     * Check consistency of meta and data part.
     * For example :
     *      elementNum
     *      groupNum
     */
    private void checkConsistency() {

        long calculatedElementNum = Arrays.stream(groups)
                .mapToLong(Group::getElementNum).sum();
        Preconditions.check(calculatedElementNum != meta.getElementNum(),
                "forward dict element num not consistent");

        long calculatedGroupNum = calculatedGroupNum();
        Preconditions.check(calculatedGroupNum != meta.getGroupNum(),
                "forward dict group num not consistent");
    }

    private void checkSum() {
        //TODO
    }

    private void checkId(long id) {
        Preconditions.check(id < 0 || id > meta.getElementNum(),
                String.format("Element id should between %d and %d", 0, meta.getElementNum()));
    }

    /**
     * TODO binary search fashion to speed up
     */
    private long findGroupId(long id) {
        long groupId = NOT_EXISTED;
        for(int i=0; i<meta.getGroupNum(); i++) {
            if(id < groupFirstElementId[i]) {
                groupId = i -1;
                break;
            }
            if(id == groupFirstElementId[i]) {
                groupId = i;
                break;
            }
        }

        if(groupId == NOT_EXISTED) {
            groupId = meta.getGroupNum() -1;
        }

        return groupId;
    }

    @Override
    @DictionaryApi
    public long elementNum() {
        return meta.getElementNum();
    }

    /**
     * Calculate group num by file length but not meta.
     */
    long calculatedGroupNum() {
        return (file.length() - ForwardDictionaryMeta.length()) / GROUP_LENGTH;
    }

    @Override
    public long id(byte[] element) {
        throw new UnsupportedOperationException("Forward dictionary doesn't support search id by element");
    }
}
