package com.oppo.tagbase.storage.core.obj;

import com.oppo.tagbase.storage.core.util.StorageConstantUtil;

import java.util.List;

/**
 * Created by liangjingya on 2020/2/14.
 */
public class DimContext {

    private int dimIndex;//标志原始的维度索引

    private int dimReturnIndex = StorageConstantUtil.FLAG_NO_NEED_RETURN;//标志返回的维度索引，-1表示不返回

    private String dimName;//标志维度名

    private List<String> dimValues;//标志取值

    public DimContext(int dimIndex, String dimName) {
        this.dimIndex = dimIndex;
        this.dimName = dimName;
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

    public List<String> getDimValues() {
        return dimValues;
    }

    public void setDimValues(List<String> dimValues) {
        this.dimValues = dimValues;
    }

    @Override
    public String toString() {
        return "DimContext{" +
                "dimIndex=" + dimIndex +
                ", dimReturnIndex=" + dimReturnIndex +
                ", dimName='" + dimName + '\'' +
                ", dimValues=" + dimValues +
                '}';
    }
}
