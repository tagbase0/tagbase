package com.oppo.tagbase.jobv2.spi;

import com.oppo.tagbase.dict.util.Uuid;
import com.oppo.tagbase.jobv2.JobConfig;
import com.oppo.tagbase.jobv2.JobUtil;
import com.oppo.tagbase.meta.obj.Props;
import com.oppo.tagbase.meta.obj.Table;
import com.oppo.tagbase.meta.obj.TableType;

import java.time.LocalDateTime;
import java.util.List;
import java.util.StringJoiner;

import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;

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

    private String uuid;

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
        this.uuid = Uuid.nextId();
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

    public TableType getTableType() {
        return table.getType();
    }

    public String getInvertedDictLocation() {
        return jobConfig.getInvertedDictPath();
    }

    public LocalDateTime getLowerBound() {
        return lowerBound;
    }

    public LocalDateTime getUpperBound() {
        return upperBound;
    }

    public List<Props> getJobProps() {
        return table.getProps();
    }

    public String getWorkDir() {
        StringJoiner joiner = new StringJoiner(JobUtil.REMOTE_STORE_FILE_SEPARATOR);
        joiner.add(jobConfig.getWorkDir());
        joiner.add(lowerBound.format(ISO_DATE_TIME));
        return joiner.toString();
    }

    public String getOutputLocation() {
        StringJoiner joiner = new StringJoiner(JobUtil.REMOTE_STORE_FILE_SEPARATOR);
        joiner.add(jobConfig.getBitmapDir());
        joiner.add(lowerBound.format(ISO_DATE_TIME));
        return joiner.toString();
    }
}
