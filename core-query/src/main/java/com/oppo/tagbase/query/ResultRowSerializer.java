package com.oppo.tagbase.query;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.oppo.tagbase.query.operator.ResultRow;

import java.io.IOException;

/**
 * @author huangfeng
 * @date 2020/2/18 19:37
 */
public class ResultRowSerializer extends JsonSerializer<ResultRow> {
    @Override
    public void serialize(ResultRow value, JsonGenerator gen, SerializerProvider serializers) throws IOException {

        gen.writeStartObject();


        gen.writeEndObject();

    }
}
