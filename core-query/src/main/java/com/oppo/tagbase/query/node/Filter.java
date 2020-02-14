package com.oppo.tagbase.query.node;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * @author huangfeng
 * @date 2020/2/7
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = Filter.IN, value = InFilter.class),
        @JsonSubTypes.Type(name = Filter.GREATER, value = GreaterFilter.class),
        @JsonSubTypes.Type(name = Filter.BELOW, value = BelowFilter.class),
})
public interface Filter {
    String IN = "in";
    String GREATER = "greater";
    String BELOW = "below";



    String getColumn();
    boolean isExact();

}