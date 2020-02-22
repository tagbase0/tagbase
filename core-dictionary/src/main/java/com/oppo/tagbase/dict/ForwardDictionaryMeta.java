package com.oppo.tagbase.dict;

import com.oppo.tagbase.common.util.BytesUtil;
import com.oppo.tagbase.common.util.Preconditions;
import com.oppo.tagbase.common.util.UnsignedTypes;

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


    public String getMagic() {
        return magic;
    }

    public void setMagic(String magic) {
        Preconditions.checkNotEquals(magic, MAGIC, "Illegal Tagbase forward dictionary");
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

    //TODO not only 1.0
    public void setVersion(String version) {
        Preconditions.checkNotEquals(version, VERSION, "Illegal version of Tagbase forward dictionary");
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
        Preconditions.checkNotEquals(bytes.length, length(), "Invalid dictionary meta length");

        ForwardDictionaryMeta meta = new ForwardDictionaryMeta();
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        String magic = BytesUtil.toUTF8String(buf, 20);
        Preconditions.checkNotEquals(magic, MAGIC, "Illegal Tagbase forward dictionary");
        meta.setMagic(magic);

        meta.setVersion(BytesUtil.toUTF8String(buf, 3));
        meta.setLastModifiedDate(buf.getLong());
        //TODO check
        meta.setCheckSum(BytesUtil.toUTF8String(buf, 8));
        // group number is designed larger than Integer.MAX_VALUE, so must use unsigned int
        meta.setGroupNum(UnsignedTypes.unsignedInt(buf.getInt()));
        meta.setElementNum(buf.getLong());

        return meta;
    }
}
