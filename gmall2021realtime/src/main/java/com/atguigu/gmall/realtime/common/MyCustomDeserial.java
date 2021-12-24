package com.atguigu.gmall.realtime.common;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class MyCustomDeserial implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //1.创建一个JSONObject来存放结果数据
        JSONObject result = new JSONObject();

        //2.获取数据库名
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String database = split[1];

        //3.获取表名
        String tableName = split[2];

        //4.获取类型insert update delete
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }

        //5.获取数据
        Struct value = (Struct) sourceRecord.value();

        //6.获取before数据
        JSONObject beforeJson = new JSONObject();
        Struct beforeStruct = value.getStruct("before");
        if (beforeStruct != null) {
            Schema schema = beforeStruct.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                beforeJson.put(field.name(), beforeStruct.get(field));
            }
        }

        //7.获取after数据
        JSONObject afterJson = new JSONObject();
        Struct afterStruct = value.getStruct("after");
        if (afterStruct != null) {
            Schema schema = afterStruct.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                afterJson.put(field.name(), afterStruct.get(field));
            }
        }

        //将数据封装到JSONObject
        result.put("database", database);
        result.put("tableName", tableName);
        result.put("before", beforeJson);
        result.put("after", afterJson);
        result.put("type", type);

        //将数据发送至下游
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
