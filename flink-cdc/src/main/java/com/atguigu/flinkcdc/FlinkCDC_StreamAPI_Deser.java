package com.atguigu.flinkcdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class FlinkCDC_StreamAPI_Deser {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_flink_0625")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeser()).build();

        env.addSource(sourceFunction).print();

        env.execute();
    }

    private static class MyDeser implements DebeziumDeserializationSchema<String> {
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            //创建一个JSONObject用来存放最终封存的数据
            JSONObject jsonObject = new JSONObject();

            //TODO 提取数据库名
            String topic = sourceRecord.topic();
            String[] split = topic.split("\\.");
            String database = split[1];

            //TODO 提取表名
            String table = split[2];


            Struct value = (Struct)sourceRecord.value();
            //TODO 获取after数据
            Struct afterStruct = value.getStruct("after");
            JSONObject afterJson = new JSONObject();
            //判断after是否为空,不为空则将元素放入afterJson
            if (afterStruct!=null) {
                List<Field> fields = afterStruct.schema().fields();
                for (Field field : fields) {
                    afterJson.put(field.name(), afterStruct.get(field));
                }
            }

            //TODO 获取before数据
            Struct beforeStruct = value.getStruct("before");
            JSONObject beforeJson = new JSONObject();
            //判断before是否为空,不为空则将元素循环放入beforeJson
            if (beforeStruct!=null) {
                List<Field> fields = beforeStruct.schema().fields();
                for (Field field : fields) {
                    beforeJson.put(field.name(), beforeStruct.get(field));
                }
            }

            //TODO 获取操作类型 DELETE CREATE UPDATE
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            String type = operation.toString().toLowerCase();
            //判断type是不是"create",如果是,替换为"insert"
            if ("create".equals(type)) {
                type = "insert";
            }

            //TODO 封住数据
            jsonObject.put("database", database);
            jsonObject.put("tableName", table);
            jsonObject.put("after", afterJson);
            jsonObject.put("before", beforeJson);
            jsonObject.put("type", type);


            collector.collect(jsonObject.toJSONString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
