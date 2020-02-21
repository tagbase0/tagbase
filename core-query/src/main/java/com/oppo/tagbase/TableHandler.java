package com.oppo.tagbase;

import com.google.common.collect.RangeSet;

import java.time.LocalDate;
import java.util.List;

/**
 * @author huangfeng
 * @date 2020/2/19 16:43
 */
public class TableHandler {

    private String dbName;
    private String tableName;

    private String sliceName;
    private RangeSet<LocalDate> sliceRange;

    private List<String>  filterColumn;
    private List<RangeSet<String>>  columnRanges;

    private List<String> dims;


}
