package com.oppo.tagbase.query.operator;

import com.google.inject.Inject;
import com.oppo.tagbase.dict.ForwardDictionary;
import com.oppo.tagbase.query.row.AggregateRow;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;


/**
 * Created by huangfeng on 2020/2/15.
 */
public class PersistResultOperator extends AbstractOperator {
    OperatorBuffer<AggregateRow> inputBuffer;
    OperatorBuffer<AggregateRow> outputBuffer;

    @Inject
    ForwardDictionary dictionary;

    public PersistResultOperator(int id,OperatorBuffer<AggregateRow> inputBuffer) {
        super(id);
        this.inputBuffer = inputBuffer;
    }

    @Override
    public void internalRun() {
        AggregateRow row;
        writeMeta();
//        Configuration configuration = null;
//
//        try (FileSystem fs = FileSystem.newInstance(configuration)) {
//            FSDataOutputStream fsd = fs.create(new Path(""));
//            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsd, "UTF-8"));
//
//
//
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        while ((row = inputBuffer.next()) != null) {
//            // write HDFS
//
//            writeRow(row);
//
//        }
//
//        outputBuffer.offer(Row.EOF);
    }

    private void writeRow(AggregateRow row) {
        ImmutableRoaringBitmap bitmap = row.getMetric();
        for (int id : bitmap) {
            byte[] imei = dictionary.element(id);

        }


    }

    private void writeMeta() {
    }
}
