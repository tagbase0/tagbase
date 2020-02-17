package com.oppo.tagbase.dict;

import com.oppo.tagbase.dict.util.UnsignedTypes;

import java.nio.ByteBuffer;

/**
 * Created by wujianchao on 2020/2/12.
 */
public class Group {

    public static final int TYPE_INT_WIDTH = 4;
    public static final int TYPE_SHORT_WIDTH = 2;

    /**
     * group length, default 64KB
     */
    public static final int GROUP_LENGTH = 2 << 15;

    private ByteBuffer data;

    private int elementSize;

    /**
     * remaining space
     */
    private int remaining = GROUP_LENGTH;

    /**
     * size of bytes an element offset in a group
     */
    @Deprecated
    private int groupOffBytes = 2;

    /**
     * meta part which include length
     * the first element offset int group
     */
    private int metaLength;

    /**
     * group total length, default 64KB
     */
    private int totalLength;

    Group(ByteBuffer data){
        this.data = data;
        this.totalLength = data.capacity();
        this.elementSize = data.getInt(0);

        calculateRemaining();
        calculateMetaLength();
    }


    public Group createGroup(ByteBuffer data) {
        return new Group(data);
    }

    //TODO add config to identify direct or JVM heap memory
    public Group createBlankGroup() {
        return createGroup(ByteBuffer.allocate(GROUP_LENGTH));
    }


    private void calculateRemaining() {
        remaining = totalLength - elementOffset(elementSize);
    }

    private void calculateMetaLength() {
        metaLength = TYPE_INT_WIDTH + elementSize * TYPE_SHORT_WIDTH;
    }

    /**
     *
     * @param id is a relative value in group
     * @return corresponding element
     */
    public byte[] element(int id){
        int elementBeginOff = elementOffset(id);
        int elementEndOff = elementOffset(id + 1);

        int length = elementEndOff - elementBeginOff;

        byte[] element = new byte[length];
        data.get(element, elementBeginOff, length);

        return element;
    }

    /**
     * Get start offset of element of id.
     * Because of sequence layout of element, elementOffset(id + 1) represent the end offset of id
     */
    private int elementOffset(int id){
        return metaLength + id==0 ? 0 :
                UnsignedTypes.unsignedShort(data.getShort(TYPE_INT_WIDTH + (id -1) * TYPE_SHORT_WIDTH));
    }


    /**
     * Get index of element of id .
     */
    public int index(int id) {
        return UnsignedTypes.unsignedShort(data.getShort(TYPE_INT_WIDTH + id * TYPE_SHORT_WIDTH));
    }


    public int getRemaining() {
        return remaining;
    }

    public int getElementSize() {
        return elementSize;
    }

    ByteBuffer getData() {
        return data;
    }
}
