package com.oppo.tagbase.job.engine.obj;

import com.oppo.tagbase.meta.obj.TaskState;

/**
 * Created by liangjingya on 2020/2/20.
 */
public class TaskMessage {

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
                '}';
    }

    public TaskState parseJobStatus() {
        if ("UNDEFINED".equals(this.finalStatus)) {
            return TaskState.valueOf(this.state);
        } else if ("FINISHED".equals(this.state)) {
            return TaskState.valueOf(this.finalStatus);
        }
        return TaskState.UNKNOWN;
    }
}
