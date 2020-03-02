package com.oppo.tagbase.query.operator;

import com.google.inject.Inject;
import com.oppo.tagbase.dict.ForwardDictionary;
import com.oppo.tagbase.extension.spi.Writer;
import com.oppo.tagbase.query.row.AggregateRow;
import com.oppo.tagbase.storage.core.obj.OperatorBuffer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * Created by huangfeng on 2020/2/15.
 */
public class PersistResultOperator extends AbstractOperator {
    private static Logger LOG = LoggerFactory.getLogger(PersistResultOperator.class);
    OperatorBuffer<AggregateRow> inputBuffer;

    @Inject
    ForwardDictionary dictionary;


    public PersistResultOperator(int id, OperatorBuffer<AggregateRow> inputBuffer) {
        super(id, new OperatorBuffer<AggregateRow>());
        this.inputBuffer = inputBuffer;

    }

    @Override
    public void internalRun() {

        AggregateRow row;
        writeMeta();


        try {
            while ((row = inputBuffer.next()) != null) {
                // write HDFS

                writeRow(row);

            }
        } catch (IOException e) {
            LOG.error("result presist fail");
            e.printStackTrace();
        }

        outputBuffer.postEnd();
    }

    Writer writer = null;

    private void writeRow(AggregateRow row) throws IOException {

        writer.write(row.getDim().toString());

        ImmutableRoaringBitmap bitmap = row.getMetric();
        for (int id : bitmap) {
            byte[] imei = dictionary.element(id);
            writer.write(","+new String(imei) );
        }


    }

    private void writeMeta() {
    }

    @Override
    public String toString() {
        return "PersistResultOperator{" + '}';
    }
}
