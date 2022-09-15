package com.atguigu.app.ods.functions;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import net.minidev.json.JSONObject;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class MyDeserializer implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        JSONObject result = new JSONObject();

        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String database = split[1];
        String table = split[2];

        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            Schema schema = before.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                beforeJson.put(field.name(), before.get(field));
            }
        }

        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            Schema schema = after.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                afterJson.put(field.name(), after.get(field));
            }
        }

        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        result.put("database", database);
        result.put("tableName", table);
        result.put("after", afterJson);
        result.put("before", beforeJson);
        result.put("type", type);

        collector.collect(result.toJSONString());

    }


    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
