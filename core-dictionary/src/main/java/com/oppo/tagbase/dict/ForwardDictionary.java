package com.oppo.tagbase.dict;

import java.io.File;

/**
 * Created by wujianchao on 2020/2/11.
 */
public class ForwardDictionary extends AbstractDictionary{

    public static final String MAGIC = "TAGBASE_FORWARD_DICT";
    public static final String VERSION = "1.0.0";

    private File file;

    private String magic;
    private String checkSum;
    private String version;

    /**
     * must yyyyMMdd format
     */
    private String lastModifiedDate;


    /**
     * data size of a group which include meta, data and remaining space.
     * recommend size: 2^16 = 64k, so groupOffsetLength = 2
     */
    private int groupLength;

    private int totalElements;

    /**
     * first element id of group in the whole dictionary
     */
    private int[] firstElementId;

    // calculated metadata

    private int totalGroups;
    private int dataStartOffset;

    /**
     * length of an element offset in a group
     */
    private int groupOffsetLength;

    /**
     * dictionary file length
     */
    private long dictLength;

    private Group[] groups;

    public ForwardDictionary(File file) {
        //TODO

    }

    @Override
    public byte[] element(int id) {
        return super.element(id);
    }

    @Override
    public int id(byte[] element) {
        throw new UnsupportedOperationException("Forward dictionary doesn't support search id by element");
    }
}
