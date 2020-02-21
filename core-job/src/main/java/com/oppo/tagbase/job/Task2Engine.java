package com.oppo.tagbase.job;

import com.oppo.tagbase.meta.obj.Slice;
import com.oppo.tagbase.meta.obj.TaskState;

import java.sql.Date;

/**
 * Created by daikai on 2020/2/21.
 */
public interface Task2Engine {

    /**
     * 在 Hdfs 上生成反向字典数据
     *
     * @param: numOld 代表当前字典的大小, dbName tableName dayno可确定出 dayno 当天 imei 增量
     * locationInvertedDictOld 代表当前反向字典文件的 Hdfs 路径
     * invertedDictNewPath 代表新生成的反向字典文件 Hdfs 存放路径
     * @return: 当前任务的执行状态
     */
    TaskState generateInvertedDictFile(long numOld, String dbName, String tableName, String dayno,
                                       String invertedDictOldPath, String invertedDictNewPath);

    /**
     * 通过反向字典和普通标签数据构建该标签的 bitMap 数据, 用 HFile 文件格式
     *
     * @param: dbName tableName 确定出标签数据, dataLowerTime 和 dataUpperTime 分别代表构建数据的时间跨度
     * invertedDictPath 代表反向字典的文件路径
     * hFilePath 代表新生成的 HFile 文件路径
     * @return: 当前任务的执行状态
     */
    TaskState generateHfile(String taskId, String dbName, String tableName, Date dataLowerTime,
                            Date dataUpperTime, String invertedDictPath, String hFilePath);


    /**
     * 将生成的 HFile 文件通过 bulkLoad 导入 Hbase, 路径为toPath
     *
     * @param:
     * @return: 根据 HFile 文件信息, 构建 Slice 并返回
     */
    Slice doBulkLoad(String hFilePath, String toPath);

    /**
     * 通过 taskid 获取当前任务的执行状态
     *
     * @param:
     * @return: TaskState
     */
    TaskState engineState(String taskId);


}
