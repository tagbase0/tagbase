package com.oppo.tagbase.extension.hdfs;

import com.oppo.tagbase.extension.spi.ResultWriter;
import com.oppo.tagbase.extension.spi.ResultWriterFactory;

import java.io.IOException;

/**
 * @author huangfeng
 * @date 2020/2/28 18:58
 */
public class HDFSWriterFactory implements ResultWriterFactory {

    @Override
    public ResultWriter createResultWriter(String filePath) throws IOException {
      return   new HDFSWriter(filePath);
    }

    @Override
    public String getName() {
        return "hdfs";
    }
}
