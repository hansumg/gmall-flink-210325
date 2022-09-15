package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//TODO 数据流： logapp => nginx => springboot => kafka(ods) => flink => kafka(dwd * 3)
//TODO 程序： app => nginx => logger.sh => kafka => baselogapp => kafa

public class BaseLogApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String sourceTopic = "ods_base_log";
        String groupId = "BaseLogApp";
        DataStreamSource<String> odsLogDS =
                env.addSource(MyKafkaUtil.getKafkaconsumer(sourceTopic, groupId));

        //TODO 将数据流转换为JSON对象，并对脏数据过滤，从侧输出流输出
        OutputTag<String> dirty = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonDS =
                odsLogDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx,
                                       Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirty, value);
                }
            }
        });

        //TODO 新老用户过滤 状态编程
        KeyedStream<JSONObject, String> keyedStream =
                jsonDS.keyBy(jsonOBJ -> jsonOBJ.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> filterDS =
                keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> vs;

            @Override
            public void open(Configuration parameters) throws Exception {
                vs = getRuntimeContext().getState(new ValueStateDescriptor<String>("vc",
                        String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                String isnew = value.getJSONObject("common").getString("is_new");

                if ("1".equals(isnew)) {
                    String st = vs.value();
                    if (st == null) {
                        vs.update("0");
                    } else {
                        value.getJSONObject("common").put("is_new", "0");
                    }
                }
                return value;
            }
        });

        //TODO 对日志进行分流输出
        OutputTag<String> startLog = new OutputTag<String>("start") {
        };
        OutputTag<String> displayLog = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageDS =
                filterDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx,
                                       Collector<String> out) throws Exception {

                JSONObject start = value.getJSONObject("start");
                if (start != null && start.size() > 0) {
                    ctx.output(startLog, value.toJSONString());
                } else {
                    out.collect(value.toJSONString());
                    JSONArray displays = value.getJSONArray("displays");
                    String id = value.getJSONObject("common").getString("id");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject displayJSON = displays.getJSONObject(i);
                            displayJSON.put("id", id);
                            ctx.output(displayLog, displayJSON.toJSONString());
                        }
                    }
                }
            }
        });

        //TODO 打印日志 并发往kafka
        jsonDS.getSideOutput(dirty).print("dirty");
        pageDS.getSideOutput(displayLog).print("display");
        pageDS.getSideOutput(startLog).print("start");
        pageDS.print("page");

        pageDS.getSideOutput(startLog).addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        pageDS.getSideOutput(displayLog).addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));
        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));

        env.execute();
    }
}
