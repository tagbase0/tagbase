package com.oppo.tagbase.jobv2.spi;

import com.oppo.tagbase.jobv2.JobConfig;
import com.oppo.tagbase.meta.obj.Table;

import java.io.File;
import java.time.LocalDateTime;
import java.util.StringJoiner;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

/**
 * Created by wujianchao on 2020/3/2.
 */
public class DataTaskContext {

    private Table table;
    private JobConfig jobConfig;
    private LocalDateTime lowerBound;
    private LocalDateTime upperBound;

    private String jobId;
    private String taskId;

    public DataTaskContext(String jobId,
                           String taskId,
                           Table table,
                           JobConfig jobConfig,
                           LocalDateTime lowerBound,
                           LocalDateTime upperBound) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.table = table;
        this.jobConfig = jobConfig;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public String getJobId() {
        return jobId;
    }

    public String getTaskId() {
        return taskId;
    }

    public Table getTable() {
        return table;
    }

    public JobConfig getJobConfig() {
        return jobConfig;
    }

    public LocalDateTime getLowerBound() {
        return lowerBound;
    }

    public LocalDateTime getUpperBound() {
        return upperBound;
    }

    public String getWorkDir() {
        StringJoiner joiner = new StringJoiner(File.separator);
        joiner.add(jobConfig.getWorkDir());
        joiner.add(jobId);
        joiner.add(taskId);
        return joiner.toString();
    }

    public String getOutputLocation() {
        StringJoiner joiner = new StringJoiner(File.separator);
        joiner.add(jobConfig.getWorkDir());
        joiner.add(jobId);
        joiner.add(taskId);
        joiner.add(lowerBound.format(ISO_LOCAL_DATE));
        return joiner.toString();
    }
}
