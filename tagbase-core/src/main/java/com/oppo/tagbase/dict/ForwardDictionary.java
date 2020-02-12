package com.oppo.tagbase.dict;

import java.nio.ByteBuffer;

/**
 * Created by wujianchao on 2020/2/11.
 */
public class ForwardDictionary extends AbstractDictionary{

    public static final String MAGIC = "TAGBASE_FORWARD_DICT";
    public static final String VERSION = "1.0.0";

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

    private int[] groupElementSizes;

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



    @Override
    public int id(byte[] v) {
        throw new UnsupportedOperationException("Forward dictionary doesn't support search id by value");
    }
}
