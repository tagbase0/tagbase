package com.oppo.tagbase.dict;

import com.oppo.tagbase.common.util.BytesUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.oppo.tagbase.dict.Group.TYPE_SHORT_WIDTH;

/**
 * Created by wujianchao on 2020/2/14.
 */
public class GroupWriter {

    public static final byte [] NULL_ELEMENT =  {2};
    public static final int GROUP_NO_ENOUGH_SPACE =  -1;

    private int elementNum;

    private List<Integer> offsets = new ArrayList<>();
    private List<byte[]> elements = new ArrayList<>();

    private Group group;

    /**
     * remaining space
     */
    private int remaining;


    public GroupWriter(Group group) {
        this.group = group;

        this.elementNum = group.getElementNum();
        this.remaining = group.getRemaining();

        fillExistedOff();
        fillExistedEle();
    }

    private void fillExistedOff(){
        for(int i = 0; i< elementNum; i++){
            offsets.add(group.index(i));
        }
    }

    private void fillExistedEle(){
        for(int i = 0; i< elementNum; i++){
            elements.add(group.element(i));
        }
    }

    public int getElementNum() {
        return elementNum;
    }

    /**
     * add an element into the group
     *
     * @return element sequence in the group, or -1 if the group has no enough space.
     */
    public int add(byte[] element){

        if(BytesUtil.isNull(element)){
            element = NULL_ELEMENT;
        }

        if(remaining < element.length){
            return GROUP_NO_ENOUGH_SPACE;
        }

        int previousElementOff = elementNum == 0 ? 0 : offsets.get(elementNum -1);
        int elementOff = previousElementOff + element.length;

        offsets.add(elementOff);
        elements.add(element);

        remaining -= (element.length + TYPE_SHORT_WIDTH);

        return elementNum++;
    }


    /**
     * serialize a group
     */
    public byte[] serialize() {

        // clear group
        ByteBuffer buffer = group.getData();
        buffer.clear();

        // rewrite data
        buffer.putInt(elementNum);

        for(int i = 0; i< elementNum; i ++) {
            buffer.putShort(offsets.get(i).shortValue());
        }

        for(int i = 0; i< elementNum; i ++) {
            buffer.put(elements.get(i));
        }

        return buffer.array();
    }

}
