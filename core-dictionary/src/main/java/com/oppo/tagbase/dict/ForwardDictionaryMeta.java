package com.oppo.tagbase.dict;

import com.oppo.tagbase.dict.util.BytesUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 *
 * Forward dictionary meta info which is head of dictionary file.
 *
 * Meat total length 51 bytes :
 *  magic : 20
 *  version : 3
 *  lastModifiedDate : 8
 *  checkSum : 8
 *  groupNum : 4
 *  totalElementNum : 8
 *
 * Created by wujianchao on 2020/2/20.
 */
public class ForwardDictionaryMeta {

    public static final String MAGIC = "TAGBASE_FORWARD_DICT";
    public static final String VERSION = "1.0";
    public static final byte[] NULL_CHECKSUM = new byte[]{0, 0, 0, 0, 0, 0, 0, 0};


    /**
     * solid string for identify a forward dict.
     */
    private String magic = MAGIC;

    /**
     * must keep length is 3
     */
    private String version = VERSION;

    /**
     * CRC check sum
     */
    private String checkSum;

    /**
     * millisecond
     */
    private long lastModifiedDate;

    /**
     * number of groups
     */
    private long groupNum = 0L;

    /**
     * number of elements
     */
    private long elementNum = 0L;


    public static String getMAGIC() {
        return MAGIC;
    }

    public static String getVERSION() {
        return VERSION;
    }

    public String getMagic() {
        return magic;
    }

    public void setMagic(String magic) {
        this.magic = magic;
    }

    public String getCheckSum() {
        return checkSum;
    }

    public void setCheckSum(String checkSum) {
        this.checkSum = checkSum;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public long getLastModifiedDate() {
        return lastModifiedDate;
    }

    public void setLastModifiedDate(long lastModifiedDate) {
        this.lastModifiedDate = lastModifiedDate;
    }

    public long getGroupNum() {
        return groupNum;
    }

    public void setGroupNum(long groupNum) {
        this.groupNum = groupNum;
    }

    public long getElementNum() {
        return elementNum;
    }

    public void setElementNum(long elementNum) {
        this.elementNum = elementNum;
    }

    public long incTotalElementNum() {
        return ++elementNum;
    }

    static int length() {
        return 51;
    }

    public byte[] serialize() {
        generateCheckSum();
        //TODO test
        ByteBuffer buf = ByteBuffer.allocate(length());
        buf.put(magic.getBytes(StandardCharsets.UTF_8));
        buf.put(version.getBytes(StandardCharsets.UTF_8));
        buf.putLong(lastModifiedDate);
        buf.put(checkSum.getBytes(StandardCharsets.UTF_8));
        buf.putInt((int) groupNum);
        buf.putLong(elementNum);
        return buf.array();
    }

    //TODO
    private void generateCheckSum() {
        checkSum = BytesUtil.toUTF8String(NULL_CHECKSUM);
    }

    /**
     * Check consistency of meta and data part.
     * For example :
     *      elementNum
     *      groupNum
     */
    public void checkConsistency() {
        //TODO
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ForwardDictionaryMeta that = (ForwardDictionaryMeta) o;
        return lastModifiedDate == that.lastModifiedDate &&
                groupNum == that.groupNum &&
                elementNum == that.elementNum &&
                Objects.equals(magic, that.magic) &&
                Objects.equals(version, that.version) &&
                Objects.equals(checkSum, that.checkSum);
    }

    @Override
    public int hashCode() {
        return Objects.hash(magic, version, checkSum, lastModifiedDate, groupNum, elementNum);
    }

    public static ForwardDictionaryMeta deserialize(byte[] bytes) {
        if(bytes.length != length()) {
            throw new DictionaryException("Invalid dictionary meta length");
        }

        ForwardDictionaryMeta meta = new ForwardDictionaryMeta();
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        String magic = BytesUtil.toUTF8String(buf, 20);
        if(!Objects.equals(magic, MAGIC)) {
            throw new DictionaryException("Illegal Tagbase forward dictionary");
        }

        meta.setMagic(magic);
        meta.setVersion(BytesUtil.toUTF8String(buf, 3));
        meta.setLastModifiedDate(buf.getLong());
        //TODO check
        meta.setCheckSum(BytesUtil.toUTF8String(buf, 8));
        meta.setGroupNum(buf.getInt());
        meta.setElementNum(buf.getLong());

        return meta;
    }
}
