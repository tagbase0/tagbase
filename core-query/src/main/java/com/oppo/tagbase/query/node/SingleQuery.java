package com.oppo.tagbase.query.node;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Created by huangfeng on 2020/2/15.
 */
public class SingleQuery implements Query {

    private String dbName;

    private String tableName;

    @JsonProperty("output")
    private OutputType outputType;

    private List<String> dimensions;

    private List<Filter> filters;

    @JsonCreator
    public SingleQuery(
            @JsonProperty("dbName") @Nullable String dbName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("output") OutputType outputType,
            @JsonProperty("values") @Nullable List<String> dimensions,
            @JsonProperty("filters") @Nullable List<Filter> filters
    ) {
        Preconditions.checkNotNull(tableName, "table name can not be null");

        this.tableName = tableName;

        this.dbName = dbName == null ? "default" : dbName;
        this.dimensions = dimensions == null ? ImmutableList.of() : dimensions;
        this.filters = filters == null ? ImmutableList.of() : filters;

        this.outputType = outputType;

    }


    public String getTableName() {
        return tableName;
    }

    public List<String> getDimensions() {
        return dimensions;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public OutputType getOutput() {
        return outputType;
    }

    @Override
    public <R> R accept(QueryVisitor<R> visitor) {
        return visitor.visitSingleQuery(this);
    }
}