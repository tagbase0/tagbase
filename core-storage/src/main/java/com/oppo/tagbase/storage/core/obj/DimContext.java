package com.oppo.tagbase.storage.core.obj;

import com.google.common.collect.RangeSet;
import com.oppo.tagbase.meta.obj.ColumnType;
import com.oppo.tagbase.storage.core.util.StorageConstant;

/**
 * Created by liangjingya on 2020/2/14.
 */
public class DimContext {

    private int dimIndex;//标志原始的维度索引

    private int dimReturnIndex = StorageConstant.FLAG_NO_NEED_RETURN;//标志返回的维度索引，-1表示不返回

    private String dimName;//标志维度名

    private RangeSet<String> dimValues;//标志取值

    private ColumnType type;//column 类型

    public DimContext(int dimIndex, String dimName, ColumnType type) {
        this.dimIndex = dimIndex;
        this.dimName = dimName;
        this.type = type;
    }

    public int getDimIndex() {
        return dimIndex;
    }

    public void setDimIndex(int dimIndex) {
        this.dimIndex = dimIndex;
    }

    public int getDimReturnIndex() {
        return dimReturnIndex;
    }

    public void setDimReturnIndex(int dimReturnIndex) {
        this.dimReturnIndex = dimReturnIndex;
    }

    public String getDimName() {
        return dimName;
    }

    public void setDimName(String dimName) {
        this.dimName = dimName;
    }

    public RangeSet<String> getDimValues() {
        return dimValues;
    }

    public void setDimValues(RangeSet<String> dimValues) {
        this.dimValues = dimValues;
    }

    public ColumnType getType() {
        return type;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "DimContext{" +
                "dimIndex=" + dimIndex +
                ", dimReturnIndex=" + dimReturnIndex +
                ", dimName='" + dimName + '\'' +
                ", dimValues=" + dimValues +
                ", type=" + type +
                '}';
    }
}
