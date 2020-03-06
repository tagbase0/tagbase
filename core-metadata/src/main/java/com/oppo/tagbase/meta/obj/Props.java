package com.oppo.tagbase.meta.obj;

/**
 * Created by wujianchao on 2020/3/5.
 */
public class Props {

    public static final String KEY_SHARD_ITEMS = "job.data.shard.items";

    private long tableId;
    private String key;
    private String value;
    private String desc;

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
