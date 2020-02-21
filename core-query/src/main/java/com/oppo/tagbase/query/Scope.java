package com.oppo.tagbase.query;

import com.google.common.collect.ImmutableMap;
import com.oppo.tagbase.query.node.OutputType;
import com.oppo.tagbase.query.operator.RowMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author huangfeng
 * @date 2020/2/18 9:52
 */
public class Scope {


    OutputType outputType;
    int outPutSize;

    List<RowMeta> outPutMeta;
    int groupMaxSize;

    public Scope(OutputType outputType, int outPutSize, List<RowMeta> outPutMeta, int groupMaxSize) {
        this.outputType = outputType;
        this.outPutSize = outPutSize;
        this.outPutMeta = outPutMeta;
        this.groupMaxSize = groupMaxSize;
    }


    public static Builder builder() {
        return new Builder();
    }

    public int getOutPutSize() {
        return outPutSize;
    }

    public OutputType getOutputType() {
        return outputType;
    }

    public List<RowMeta> getOutRelations() {

        return outPutMeta;
    }


    public Map<String, RowMeta> getOutPutMeta() {
        ImmutableMap.Builder builder = ImmutableMap.builder();

        outPutMeta.forEach(item -> builder.put(item.getID(), item));
        return builder.build();
    }

    public int getGroupMaxSize() {
        return groupMaxSize;
    }


    public static class Builder {

        OutputType outputType;
        int outPutSize = Integer.MAX_VALUE;
        List<RowMeta> outPutMeta = new ArrayList<>();
        int groupMaxSize;


        public Builder withroupMaxSize(int groupMaxSize) {
            this.groupMaxSize = groupMaxSize;
            return this;
        }

        public Builder withOutputType(OutputType output) {
            this.outputType = output;
            return this;
        }

        public Builder addRowMeta(RowMeta rowMeta) {
            outPutMeta.add(rowMeta);
            return this;
        }

        public Builder withOutputSize(int size) {
            this.outPutSize = size;
            return this;
        }

        public Scope build() {
            return new Scope(outputType, outPutSize, outPutMeta, groupMaxSize);
        }
    }
}