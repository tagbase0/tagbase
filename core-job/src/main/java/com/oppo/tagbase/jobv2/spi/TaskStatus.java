package com.oppo.tagbase.jobv2.spi;

import java.util.Objects;

/**
 * Created by wujianchao on 2020/2/26.
 */
public class TaskStatus {

    private String diagnostics;

    private String finalStatus;

    private String state;

    private String name;

    private String user;

    private String queue;

    private long startedTime;

    private long finishedTime;

    public String getFinalStatus() {
        return finalStatus;
    }

    public void setFinalStatus(String finalStatus) {
        this.finalStatus = finalStatus;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public long getStartedTime() {
        return startedTime;
    }

    public void setStartedTime(long startedTime) {
        this.startedTime = startedTime;
    }

    public long getFinishedTime() {
        return finishedTime;
    }

    public void setFinishedTime(long finishedTime) {
        this.finishedTime = finishedTime;
    }

    @Override
    public String toString() {
        return "TaskMessage{" +
                "finalStatus='" + finalStatus + '\'' +
                ", state='" + state + '\'' +
                ", name='" + name + '\'' +
                ", user='" + user + '\'' +
                ", queue='" + queue + '\'' +
                ", startedTime=" + startedTime +
                ", finishedTime=" + finishedTime +
                ", diagnostics=" + diagnostics +
                '}';
    }

    public boolean isDone() {
        return Objects.equals(state, "FINISHED");
    }

    public boolean isSuccess() {
        return Objects.equals(finalStatus, "SUCCEEDED");
    }

    public String getDiagnostics() {
        return diagnostics;
    }

    public void setDiagnostics(String diagnostics) {
        this.diagnostics = diagnostics;
    }
}
