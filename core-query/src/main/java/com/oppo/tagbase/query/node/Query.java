package com.oppo.tagbase.query.node;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * @author huangfeng
 * @date 2020/2/7
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "queryType")
@JsonSubTypes(value = {
        @JsonSubTypes.Type(name = Query.COMPLEX, value = ComplexQuery.class),
        @JsonSubTypes.Type(name = Query.SINGLE, value = SingleQuery.class),
})
public interface Query {
    String COMPLEX = "complex";
    String SINGLE = "single";


    <R> R accept(QueryVisitor<R> visitor);
}
