package com.oppo.tagbase.job_v2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.oppo.tagbase.common.guice.Config;

import java.util.Objects;

/**
 *
 * Dict input type is Hive.
 * TODO make it pluggable
 *
 * Created by wujianchao on 2020/2/27.
 */
@Config("tagbase.job.dict.input.hive")
public class DictHiveInputConfig {

    @JsonProperty("db")
    private String dbName;

    @JsonProperty("table")
    private String tableName;

    @JsonProperty("column")
    private String column;

    @JsonProperty("part_column")
    private String partitionColumn;

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumn() {
        return column;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DictHiveInputConfig that = (DictHiveInputConfig) o;
        return Objects.equals(dbName, that.dbName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(column, that.column) &&
                Objects.equals(partitionColumn, that.partitionColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, tableName, column, partitionColumn);
    }
}
