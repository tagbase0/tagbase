package com.oppo.tagbase.query.node;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.RangeSet;

/**
 * Created by huangfeng on 2020/2/15.
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = Filter.IN, value = InFilter.class),
        @JsonSubTypes.Type(name = Filter.BOUND, value = BoundFilter.class)
})
public interface Filter {
    String IN = "in";
    String BOUND = "bound";

    String getColumn();

    boolean isExact();

    RangeSet<String> getDimensionRangeSet();

}