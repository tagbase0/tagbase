package com.oppo.tagbase.dict;

import java.nio.ByteBuffer;

/**
 * Created by wujianchao on 2020/2/12.
 */
public class Group {

    private int elementSize;
    private int[] offsets;

    /**
     * remaining space
     */
    private int remaining;
    private ByteBuffer data;

    /**
     * length of an element offset in a group
     */
    private int groupOffsetLength;

    public Group(ByteBuffer data){
        this.data = data;
        this.elementSize = data.getInt();
    }

}
