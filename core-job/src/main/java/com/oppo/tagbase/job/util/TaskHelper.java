package com.oppo.tagbase.job.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.oppo.tagbase.dict.ForwardDictionaryWriter;
import com.oppo.tagbase.meta.MetadataDict;
import com.oppo.tagbase.meta.obj.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.sql.Date;
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

        // 1.先读取反向字典相关数据, 将今日新增数据生成<id, imei>的 kv 结构
        TreeMap<Long, String> mapInverted = readInvertedHDFS(invertedDictHDFSPath, numOld);

        // 2.将正向字典数据拉至本地
        String dictForwardOldPath = dictForwardOld.getLocation();
        Date date = new Date(System.currentTimeMillis());
        String forwardDictDiskPath = "/tmp/tagBase/forwardDictLocal_" + date + ".txt";

        File fileLocal = new File(forwardDictDiskPath);
        if (fileLocal.exists()) {
            fileLocal.delete();
        }

        copy2Disk(dictForwardOldPath, forwardDictDiskPath);

        // 3.将新增数据追加写入正向字典数据本地磁盘文件, 并构建字典
        write2Disk(mapInverted, forwardDictDiskPath);

        try {
            if (firstBuild()) {
                ForwardDictionaryWriter.createWriter(new File(forwardDictDiskPath));
            } else {
                ForwardDictionaryWriter.createWriterForExistedDict(new File(forwardDictDiskPath));
            }

        } catch (IOException e) {
            log.error("Error to ForwardDictionaryWriter !");
        }

        // 4.将更新正向字典数据 Hdfs 文件
//        String locationHdfsForwardOld = dictForwardOld.getLocation();
//        write2HDFS(mapInverted, locationHdfsForwardOld, forwardDictHDFSPath);
        copyFile2HDFS(forwardDictDiskPath, forwardDictHDFSPath);

        // 5.更新字典元数据
        dict.setLocation(forwardDictHDFSPath);
        dict.setCreateDate(new Date(System.currentTimeMillis()));
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

    private boolean firstBuild() {
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
                // 此处确定imei和id的分隔符
                if (line.contains(" ")) {
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

}
