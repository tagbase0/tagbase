package com.oppo.tagbase.query;

import com.oppo.tagbase.meta.type.DataType;
import com.oppo.tagbase.query.node.BoundFilter;
import com.oppo.tagbase.query.node.Filter;
import com.oppo.tagbase.query.node.InFilter;

import java.util.Iterator;

/**
 * @author huangfeng
 * @date 2020/2/19 14:44
 */
public class FilterAnalyzer {
    public void analyze(Filter filter, DataType dataType) {
        if (filter instanceof InFilter) {

            Iterator<String> values = ((InFilter) filter).getValues().iterator();


            while (values.hasNext()){

            }


        } else if (filter instanceof BoundFilter) {


        }

    }
}
