package com.oppo.tagbase.job;

import java.util.concurrent.*;

/**
 * Created by daikai on 2020/2/16.
 */
public class AbstractJob implements Job {

    private String id;
    private long sliceId;
    private long tableId;
    private String jobName;
    private long startTime;
    private long runtime;
    private long endTime;
    private String lastestTask;
    private JobType type;
    private State state;

    protected static ConcurrentLinkedQueue<AbstractJob> PENDING_JOBS_QUEUE = new ConcurrentLinkedQueue<>();
    protected static ConcurrentLinkedQueue<AbstractJob> RUNNING_JOBS_QUEUE = new ConcurrentLinkedQueue<>();

    static ExecutorService JOB_EXECUTORS = new ThreadPoolExecutor(
            200,
            500,
            60L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1000),
            new ThreadPoolExecutor.AbortPolicy());

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }


    public long getRuntime() {
        return runtime;
    }

    public void setRuntime(long runtime) {
        this.runtime = runtime;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public long getSliceId() {
        return sliceId;
    }

    public void setSliceId(long sliceId) {
        this.sliceId = sliceId;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getLastestTask() {
        return lastestTask;
    }

    public void setLastestTask(String lastestTask) {
        this.lastestTask = lastestTask;
    }

    public JobType getType() {
        return type;
    }

    public void setType(JobType type) {
        this.type = type;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }


    @Override
    public boolean succeed(String jobId) {
        return false;
    }

    @Override
    public String build(String dbName, String tableName) {
        return null;
    }

    @Override
    public String buildDict(String dbName, String tableName) {
        return null;
    }

    public void addPendingJob(AbstractJob abstractJob){
        PENDING_JOBS_QUEUE.add(abstractJob);
    }

    public AbstractJob getPendingJob(AbstractJob abstractJob){
        return PENDING_JOBS_QUEUE.poll();
    }

    public void addRunningJob(AbstractJob abstractJob){
        RUNNING_JOBS_QUEUE.add(abstractJob);
    }

    public AbstractJob getRunningJob(AbstractJob abstractJob){
        return RUNNING_JOBS_QUEUE.poll();
    }

    public boolean checkBuildState(){
        return true;
    }


}
