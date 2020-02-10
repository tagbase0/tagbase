package com.oppo.tagbase.query.node;


/**
 * @author huangfeng
 * @date 2020/2/10 14:54
 */
public interface QueryVisitor<R> {

     R visitSingleQuery(SingleQuery query);
     R visitComplexQuery(ComplexQuery query);
}
