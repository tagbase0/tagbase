package com.oppo.tagbase.job.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oppo.tagbase.dict.ForwardDictionaryWriter;
import com.oppo.tagbase.job.engine.obj.HiveMeta;
import com.oppo.tagbase.job.engine.obj.HiveSrcTable;
import com.oppo.tagbase.meta.Metadata;
import com.oppo.tagbase.meta.MetadataDict;
import com.oppo.tagbase.meta.obj.Dict;
import com.oppo.tagbase.meta.obj.DictStatus;
import com.oppo.tagbase.meta.obj.DictType;
import com.oppo.tagbase.meta.obj.Slice;
import com.oppo.tagbase.meta.obj.Task;
import com.oppo.tagbase.meta.obj.TaskState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by daikai on 2020/2/19.
 */
public class TaskHelper {

    Logger log = LoggerFactory.getLogger(TaskHelper.class);

    /**
     * 检查这个任务的前置任务是否正常执行成功
     */
    public boolean preTaskFinish(List<Task> tasks, int step) {

        for (int i = 0; i < step; i++) {
            if (TaskState.SUCCESS != tasks.get(i).getState()) {
                return false;
            }
        }
        return true;
    }


    public Dict buildDictForward(String invertedDictHDFSPath, Dict dictForwardOld, String forwardDictHDFSPath) {

        Dict dict = new Dict();

        long numOld = dictForwardOld.getElementCount();

        // 1. get the new data of the invertedDict
        TreeMap<Long, String> mapInverted = readInvertedHDFS(invertedDictHDFSPath, numOld);

        // 2. get the forwardDict data from HDFS
        String dictForwardOldHDFSPath = dictForwardOld.getLocation();
        Date date = new Date(System.currentTimeMillis());
        String forwardDictDiskPath = "/tmp/tagBase/forwardDictLocal_" + date + ".txt";

        File fileLocal = new File(forwardDictDiskPath);
        if (fileLocal.exists()) {
            fileLocal.delete();
        }

        copy2Disk(dictForwardOldHDFSPath, forwardDictDiskPath);

        // 3. update forwardDict file on local disk
        write2Disk(mapInverted, forwardDictDiskPath);

        try {
            if (firstBuildDict()) {
                ForwardDictionaryWriter.createWriter(new File(forwardDictDiskPath));
            } else {
                ForwardDictionaryWriter.createWriterForExistedDict(new File(forwardDictDiskPath));
            }

        } catch (IOException e) {
            log.error("Error to ForwardDictionaryWriter !");
        }

        // 4. update forwardDict file on HDFS
//        String locationHdfsForwardOld = dictForwardOld.getLocation();
//        write2HDFS(mapInverted, locationHdfsForwardOld, forwardDictHDFSPath);
        copyFile2HDFS(forwardDictDiskPath, forwardDictHDFSPath);

        // 5. update forwardDict Metadata
        dict.setLocation(forwardDictHDFSPath);
        dict.setCreateDate(LocalDateTime.now());
        dict.setStatus(DictStatus.READY);
        dict.setElementCount(numOld + mapInverted.size());
        dict.setId(System.currentTimeMillis());
        dict.setType(DictType.FORWARD);
        dict.setVersion("");

        return dict;

    }

    private void copyFile2HDFS(String forwardDictDiskPath, String forwardDictHDFSPath) {

        Configuration conf = new Configuration();

        try (FileSystem fs = FileSystem.get(conf)) {

            if (!fs.exists(new Path(forwardDictHDFSPath))) {
                fs.mkdirs(new Path(forwardDictHDFSPath));
            }

            fs.copyFromLocalFile(new Path(forwardDictDiskPath), new Path(forwardDictHDFSPath));


        } catch (Exception e) {
            log.error("Error copy forward dictionary data from local disk to HDFS!");
        }
    }

    private boolean firstBuildDict() {

        if (new MetadataDict().getDictElementCount() == 0) {
            return true;
        }
        return false;
    }

    private void copy2Disk(String dest, String local) {
        Configuration conf = new Configuration();
        try (
                FileSystem fs = FileSystem.get(URI.create(dest), conf);
                FSDataInputStream fsdi = fs.open(new Path(dest));
                OutputStream output = new FileOutputStream(local)) {

            IOUtils.copyBytes(fsdi, output, 4096, true);

        } catch (Exception e) {
            log.error("Error copy forward dictionary data from Hdfs to local !");
        }

    }

    private void write2Disk(TreeMap<Long, String> mapInverted, String locationForwardDisk) {
        try {
            for (Map.Entry<Long, String> entry : mapInverted.entrySet()) {
                // 打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
                String str = entry.getValue();
                FileWriter writer = new FileWriter(locationForwardDisk, true);
                writer.write(System.getProperty("line.separator") + str);
                writer.close();
            }

        } catch (IOException e) {
            log.error("Error to write forward dictionary disk file ! ");
        }
    }

    public TreeMap readInvertedHDFS(String locationInverted, long numOld) {
        TreeMap<Long, String> mapInverted = new TreeMap<>();
        try (FileSystem fs = FileSystem.get(URI.create(locationInverted), new Configuration());
             FSDataInputStream fsr = fs.open(new Path(locationInverted));
             BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsr))) {
            String line;
            String imei;
            long id;
            while ((line = bufferedReader.readLine()) != null) {
                //TODO 此处确定imei和id的分隔符
                if (line.contains(",")) {
                    imei = line.split(" ")[0];
                    id = Long.parseLong(line.split(" ")[1]);
                    if (numOld <= id) {
                        mapInverted.put(id, imei);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Read inverted dictionary from hdfs ERROR !");
        }
        return mapInverted;
    }

    public void write2HDFS(Map<Long, String> mapInverted, String locationHDFSForwardOld, String locationHDFSForwardNew) {

        Path pathSrc = new Path(locationHDFSForwardOld);
        Path pathDes = new Path(locationHDFSForwardNew);
        Configuration conf = new Configuration();
        try (FileSystem fs = pathDes.getFileSystem(conf);
             FSDataOutputStream output = fs.append(pathDes)) {

            // 1.复制旧的内容
            FileUtil.copy(fs, pathSrc,
                    fs, pathDes,
                    false, true, conf);

            ObjectMapper objectMapper = new ObjectMapper();

            //如果此文件不存在则创建新文件
            if (!fs.exists(pathDes)) {
                fs.createNewFile(pathDes);
            }

            // 2.追加新的内容
            for (Map.Entry<Long, String> entry : mapInverted.entrySet()) {
                output.write(objectMapper.writeValueAsString(entry.getValue()).getBytes("UTF-8"));
                //换行
                output.write(System.getProperty("line.separator").getBytes("UTF-8"));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public Slice constructSlice(HiveMeta hiveMeta, String sink) {
        Slice slice = new Slice();
        Long sliceId = System.currentTimeMillis();
        HiveSrcTable hiveSrcTable = hiveMeta.getHiveSrcTable();
        long tableId = new Metadata().getTable(hiveSrcTable.getDbName(), hiveSrcTable.getTableName()).getId();
        Date startTime = null;
        Date endTime = null;

        try {
            startTime = new Date(new SimpleDateFormat("yyyyMMdd").
                    parse(hiveMeta.getHiveSrcTable().getSliceColumn().getColumnValueLeft()).getTime());
            endTime = new Date(new SimpleDateFormat("yyyyMMdd").
                    parse(hiveMeta.getHiveSrcTable().getSliceColumn().getColumnValueRight()).getTime());

        } catch (ParseException e) {
            log.warn("Error convert to date when in constructSlice");
        }

        String rowCountOutHDFS = hiveMeta.getRowCountPath();
        long srcCount = getHDFSOutCount(rowCountOutHDFS, 0);
        long sinkCount = getHDFSOutCount(rowCountOutHDFS, 1);

        slice.setId(sliceId);
        slice.setTableId(tableId);
        slice.setStartTime(LocalDateTime.now());
        slice.setEndTime(LocalDateTime.now());
        slice.setSink(sink);
        slice.setSrcCount(srcCount);
        slice.setSinkCount(sinkCount);


        return slice;
    }

    private long getHDFSOutCount(String rowCountOutHDFS, int row) {

        long count = -1L;

        try (FileSystem fs = FileSystem.get(URI.create(rowCountOutHDFS), new Configuration());
             FSDataInputStream fsr = fs.open(new Path(rowCountOutHDFS));
             BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsr))) {
            String line;

            int i = 0;
            while ((line = bufferedReader.readLine()) != null) {
                //TODO 此处确定imei和id的分隔符
                if (line.contains(",") && row == i) {
                    count = Long.parseLong(line.split(",")[1].trim());
                }
                i++;
            }
        } catch (Exception e) {
            log.error("Read inverted dictionary from hdfs ERROR !");
        }
        return count;
    }
}
