package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.ods.functions.DimSinkFunction;
import com.atguigu.app.ods.functions.MyDeserializer;
import com.atguigu.app.ods.functions.TableProcessFuntion;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDBApp {

    public static void main(String[] args) throws Exception {

        //TODO 获取执行环境
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());

        //TODO 消费kafka ods_base_db主题数据创建流
        String topic = "ods_base_db";
        String groupID = "Base_DB_App";
        DataStreamSource<String> odsDBDS =
                env.addSource(MyKafkaUtil.getKafkaconsumer(topic, groupID));

        //TODO 将数据转换为json对象，并过滤（delete数据）
        SingleOutputStreamOperator<JSONObject> jsonOBJDS = odsDBDS.map(new MapFunction<String,
                JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });

        SingleOutputStreamOperator<JSONObject> filterDS =
                jsonOBJDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String type = value.getString("type");
                return !"delete".equals(type);
            }
        });

        //TODO 使用FlinkCDC 消费配置表并处理成广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall2021_realtime")
                .tableList("gmall2021_realtime.table_process")
                .deserializer(new MyDeserializer())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);

        //TODO 将广播流处理为JAVABean
        SingleOutputStreamOperator<TableProcess> processDS =
                tableProcessDS.map(new MapFunction<String, TableProcess>() {
            @Override
            public TableProcess map(String value) throws Exception {
                JSONObject json = JSON.parseObject(value);
                String after = json.getString("after");
                return JSON.parseObject(after, TableProcess.class);
            }
        });
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map",
                String.class, TableProcess.class);
        BroadcastStream<TableProcess> broadcastStream = processDS.broadcast(mapStateDescriptor);

        //TODO 连接主流和广播流
        BroadcastConnectedStream<JSONObject, TableProcess> connect =
                filterDS.connect(broadcastStream);

        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("side") {
        };

        //TODO 分流 处理数据
        SingleOutputStreamOperator<JSONObject> result =
                connect.process(new TableProcessFuntion(mapStateDescriptor, outputTag));

        //TODO 打印测试
        result.getSideOutput(outputTag).print("HBase");
        result.print("kafka");

        //TODO 将数据写入Hbase
        result.getSideOutput(outputTag).addSink(new DimSinkFunction());

        //TODO 将数据写入Kafka
        result.addSink(MyKafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element,
                                                            @Nullable Long timestamp) {
                return new ProducerRecord<>(
                        element.getString("sinkTable"),
                        element.getString("after").getBytes()
                );
            }
        }));



        env.execute();


    }

}
