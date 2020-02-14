package com.oppo.tagbase.query.operator;


public interface Row {
    AggregateRow EOF = new AggregateRow();
    Dimensions getDim();

}
