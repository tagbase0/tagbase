package com.oppo.tagbase.jobv2;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.oppo.tagbase.meta.obj.Job;
import com.oppo.tagbase.meta.obj.Slice;
import com.oppo.tagbase.meta.obj.Task;
import com.oppo.tagbase.meta.obj.TaskState;

import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Created by wujianchao on 2020/2/28.
 */
public class JobUtil {

    public static final String BUILDING_BITMAP_TASK = "BUILDING_BITMAP_TASK";
    public static final String LOAD_BITMAP_TO_STORAGE_TASK = "LOAD_BITMAP_TO_STORAGE_TASK";

    public static final String BUILDING_INVERTED_DICT_TASK = "BUILDING_INVERTED_DICT_TASK";
    public static final String BUILDING_FORWARD_DICT_TASK = "BUILDING_FORWARD_DICT_TASK";

    public static String taskName(Job job, Task task) {
        return format("%s - step %d", job.getName(), task.getStep());
    }

    public static Task previousTask(List<Task> taskList, Task current) {
        byte step = current.getStep();
        if(step == 0) {
            return null;
        }
        return taskList.get(step - 1);
    }

    public static Task previousTask(Job job, Task current) {
        return previousTask(job.getTasks(), current);
    }

    public static void addTasksToJob(Job job) {

        switch (job.getType()) {
            case DATA:
                makeDataJobTasks(job);
                break;
            case DICTIONARY:
                makeDictJobTasks(job);
                break;
            default:
                throw new JobException("Illegal job type: " + job.getType().name());
        }
    }

    private static void makeDataJobTasks(Job job) {
        Task buildingBitmapTask = new Task();
        buildingBitmapTask.setState(TaskState.PENDING);
        buildingBitmapTask.setStep((byte) 0);
        buildingBitmapTask.setName(BUILDING_BITMAP_TASK);
        buildingBitmapTask.setId(job.getId());

        Task loadDataToStorageTask = new Task();
        loadDataToStorageTask.setState(TaskState.PENDING);
        loadDataToStorageTask.setStep((byte) 1);
        loadDataToStorageTask.setName(LOAD_BITMAP_TO_STORAGE_TASK);
        loadDataToStorageTask.setId(job.getId());

        job.setTasks(Lists.newArrayList(buildingBitmapTask, loadDataToStorageTask));
    }

    private static void makeDictJobTasks(Job job) {
        Task buildingInvertedDictTask = new Task();
        buildingInvertedDictTask.setState(TaskState.PENDING);
        buildingInvertedDictTask.setStep((byte) 0);
        buildingInvertedDictTask.setName(BUILDING_INVERTED_DICT_TASK);
        buildingInvertedDictTask.setId(job.getId());

        Task loadDataToStorageTask = new Task();
        loadDataToStorageTask.setState(TaskState.PENDING);
        loadDataToStorageTask.setStep((byte) 1);
        loadDataToStorageTask.setName(BUILDING_FORWARD_DICT_TASK);
        loadDataToStorageTask.setId(job.getId());

        job.setTasks(Lists.newArrayList(buildingInvertedDictTask, loadDataToStorageTask));
    }

    public static String makeForwardDictName() {
        return System.getProperty("user.dir")
                + File.separator
                + "dict"
                + File.separator
                + "dict-forward-"
                + LocalDate.now()
                + ThreadLocalRandom.current().nextInt(100)
                ;
    }

    public static String makeDictJobName(LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {
        return Joiner.on("_").join("DICt", dataLowerTime, dataUpperTime);
    }

    public static String makeDataJobName(String dbName, String tableName, LocalDateTime dataLowerTime, LocalDateTime dataUpperTime) {
        return Joiner.on("_").join(dbName, tableName, dataLowerTime, dataUpperTime);
    }

    public static Timeline makeJobTimeline(List<Job> jobList) {
        return Timeline.of(jobList.stream()
                .sorted()
                .map(job -> Range.closedOpen(job.getDataLowerTime(), job.getDataUpperTime()))
                .collect(Collectors.toCollection(TreeSet::new)));
    }

    public static Timeline makeSliceTimeline(List<Slice> sliceList) {
        return Timeline.of(sliceList.stream()
                .sorted()
                .map(job -> Range.closedOpen(job.getStartTime(), job.getEndTime()))
                .collect(Collectors.toCollection(TreeSet::new)));
    }
}
