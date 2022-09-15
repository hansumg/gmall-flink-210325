package com.atguigu.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.ods.functions.MyDeserializer;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-210325-flink")
                .deserializer(new MyDeserializer())
                .startupOptions(StartupOptions.latest())
                .build();

        DataStreamSource<String> mysqlSource = env.addSource(sourceFunction);
        String sinkTopic = "ods_base_db";
        mysqlSource.addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));
        env.execute();
    }
}
