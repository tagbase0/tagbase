package com.oppo.tagbase.dict;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wujianchao on 2020/2/20.
 */
public class GroupMeta {

    public static final int TYPE_INT_WIDTH = 4;
    public static final int TYPE_SHORT_WIDTH = 2;

    /**
     * group length, default 64KB
     */
    public static final int GROUP_LENGTH = 2 << 15;

    /**
     * element offset takes 2 bytes
     */
    public static final int OFFSET_BYTES = 2;

    private int elementNum = 0;
    private List<Integer> elementOffsetList = new ArrayList<>();

    /**
     * remaining space
     */
    private int remaining = GROUP_LENGTH;

    public int getElementNum() {
        return elementNum;
    }

    public void setElementNum(int elementNum) {
        this.elementNum = elementNum;
    }

    public List<Integer> getElementOffsetList() {
        return elementOffsetList;
    }

    public void addElementOffsetList(int offset) {
        this.elementOffsetList.add(offset);
    }

    public int getRemaining() {
        return remaining;
    }

    public void setRemaining(int remaining) {
        this.remaining = remaining;
    }

    public int metaLength() {
        return TYPE_INT_WIDTH + OFFSET_BYTES * elementOffsetList.size();
    }


}
