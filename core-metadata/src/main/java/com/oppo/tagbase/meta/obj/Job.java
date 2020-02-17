package com.oppo.tagbase.meta.obj;

import java.sql.Date;
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
    private Date startTime;
    private Date endTime;
    private Date dataLowerTime; // include
    private Date dataUpperTime; // exclude
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

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public Date getDataLowerTime() {
        return dataLowerTime;
    }

    public void setDataLowerTime(Date dataLowerTime) {
        this.dataLowerTime = dataLowerTime;
    }

    public Date getDataUpperTime() {
        return dataUpperTime;
    }

    public void setDataUpperTime(Date dataUpperTime) {
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
