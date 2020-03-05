package com.oppo.tagbase.jobv2.spi;

import com.oppo.tagbase.jobv2.DictHiveInputConfig;
import com.oppo.tagbase.jobv2.JobConfig;

import java.io.File;
import java.time.LocalDateTime;
import java.util.StringJoiner;

import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;

/**
 * Created by wujianchao on 2020/3/2.
 */
public class DictTaskContext {

    private DictHiveInputConfig dictHiveInputConfig;
    private JobConfig jobConfig;
    private long nextId;
    private LocalDateTime lowerBound;
    private LocalDateTime upperBound;

    private String jobId;
    private String taskId;

    public DictTaskContext(String jobId,
                           String taskId,
                           DictHiveInputConfig dictHiveInputConfig,
                           JobConfig jobConfig,
                           long nextId,
                           LocalDateTime lowerBound,
                           LocalDateTime upperBound) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.dictHiveInputConfig = dictHiveInputConfig;
        this.jobConfig = jobConfig;
        this.nextId = nextId;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    public String getJobId() {
        return jobId;
    }

    public String getTaskId() {
        return taskId;
    }

    public DictHiveInputConfig getDictHiveInputConfig() {
        return dictHiveInputConfig;
    }

    public long getNextId() {
        return nextId;
    }

    public LocalDateTime getLowerBound() {
        return lowerBound;
    }

    public LocalDateTime getUpperBound() {
        return upperBound;
    }

    /**
     * @return remote engine working dir
     */
    public String getWorkDir() {
        StringJoiner joiner = new StringJoiner(File.separator);
        joiner.add(jobConfig.getWorkDir());
        joiner.add(jobId);
        joiner.add(taskId);
        return joiner.toString();
    }

    /**
     * @return  increasing inverted dict location
     */
    public String getOutputLocation() {
        StringJoiner joiner = new StringJoiner(File.separator);
        joiner.add(jobConfig.getInvertedDictPath());
        joiner.add(lowerBound.format(ISO_DATE_TIME));
        return joiner.toString();
    }
}
