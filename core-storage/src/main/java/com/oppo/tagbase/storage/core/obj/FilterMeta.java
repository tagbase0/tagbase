package com.oppo.tagbase.storage.core.obj;

import java.util.List;

/**
 * Created by liangjingya on 2020/2/14.
 */
public class FilterMeta {

    private int index;//标志原始的维度索引

    private String dimensions;//标志维度

    private List<String> values;//标志取值

    private int returnIndex = -1;//标志返回的维度索引

    public FilterMeta(int index, String dimensions) {
        this.index = index;
        this.dimensions = dimensions;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getDimensions() {
        return dimensions;
    }

    public void setDimensions(String dimensions) {
        this.dimensions = dimensions;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public int getReturnIndex() {
        return returnIndex;
    }

    public void setReturnIndex(int returnIndex) {
        this.returnIndex = returnIndex;
    }
}
