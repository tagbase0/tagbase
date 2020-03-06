package com.oppo.tagbase.jobv2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.oppo.tagbase.common.guice.Config;
import com.oppo.tagbase.meta.obj.ColDateFormat;

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

    @JsonProperty("column.type")
    private String columnType;

    @JsonProperty("part_column")
    private String partitionColumn;

    @JsonProperty("part_column.type")
    private String partitionColumnType;

    @JsonProperty("part_column.format")
    private ColDateFormat partitionColumnFormat;

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumn() {
        return column;
    }

    public String getColumnType() {
        return columnType;
    }

    public String getPartitionColumn() {
        return partitionColumn;
    }

    public String getPartitionColumnType() {
        return partitionColumnType;
    }

    public ColDateFormat getPartitionColumnFormat() {
        return partitionColumnFormat;
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
                Objects.equals(columnType, that.columnType) &&
                Objects.equals(partitionColumn, that.partitionColumn) &&
                Objects.equals(partitionColumnType, that.partitionColumnType) &&
                Objects.equals(partitionColumnFormat, that.partitionColumnFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName,
                tableName,
                column,
                columnType,
                partitionColumn,
                partitionColumnType,
                partitionColumnFormat);
    }
}
