package com.oppo.tagbase.storage.core.connector;

import java.util.List;

/**
 * Created by liangjingya on 2020/2/14.
 */
public class StroageFilter {

    private int index;//标志维度顺序

    private boolean flag;//标志是否需要返回

    private String dimensions;//标志维度

    private List<String> values;//标志取值

    public StroageFilter(int index, String dimensions) {
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

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }
}
