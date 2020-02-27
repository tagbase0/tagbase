package com.oppo.tagbase.meta.obj;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

/**
 * Created by wujianchao on 2020/2/17.
 */
public class Job {

    /**
     * uuid
     */
    private String id;
    private String name;
    private String dbName;
    private String tableName;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private LocalDateTime dataLowerTime; // include
    private LocalDateTime dataUpperTime; // exclude
    private String latestTask;
    private JobState state;
    private JobType type;

    private List<Task> tasks;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    public LocalDateTime getDataLowerTime() {
        return dataLowerTime;
    }

    public void setDataLowerTime(LocalDateTime dataLowerTime) {
        this.dataLowerTime = dataLowerTime;
    }

    public LocalDateTime getDataUpperTime() {
        return dataUpperTime;
    }

    public void setDataUpperTime(LocalDateTime dataUpperTime) {
        this.dataUpperTime = dataUpperTime;
    }

    public String getLatestTask() {
        return latestTask;
    }

    public void setLatestTask(String latestTask) {
        this.latestTask = latestTask;
    }

    public JobState getState() {
        return state;
    }

    public void setState(JobState state) {
        this.state = state;
    }

    public JobType getType() {
        return type;
    }

    public void setType(JobType type) {
        this.type = type;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Job job = (Job) o;
        return Objects.equals(id, job.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
