package com.oppo.tagbase.meta.obj;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Created by wujianchao on 2020/2/17.
 */
public class Task {

    /**
     * uuid
     */
    private String id;
    private String name;
    private String jobId;
    private String appId;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private byte step;
    private TaskState state;
    private String output;

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

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
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

    public byte getStep() {
        return step;
    }

    public void setStep(byte step) {
        this.step = step;
    }

    public TaskState getState() {
        return state;
    }

    public void setState(TaskState state) {
        this.state = state;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    @Override
    public Task clone() {
        Task task = new Task();
        task.setId(id);
        task.setName(name);
        task.setEndTime(endTime);
        task.setStartTime(startTime);
        task.setAppId(appId);
        task.setStep(step);
        task.setJobId(jobId);
        task.setState(state);
        task.setOutput(output);
        return task;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Task task = (Task) o;
        return Objects.equals(id, task.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
