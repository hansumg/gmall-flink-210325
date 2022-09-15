package com.atguigu.app.ods.functions;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFuntion extends BroadcastProcessFunction<JSONObject, TableProcess,JSONObject> {

    private MapStateDescriptor<String,TableProcess> mapStateDescriptor;
    private PreparedStatement ps;
    private OutputTag<JSONObject> outputTag;

    public TableProcessFuntion(MapStateDescriptor<String, TableProcess> mapStateDescriptor,
                               OutputTag<JSONObject> outputTag) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.outputTag = outputTag;
    }

    @Override
    public void processBroadcastElement(TableProcess value, Context ctx,
                                        Collector<JSONObject> out) throws Exception {
        if (TableProcess.SINK_TYPE_HBASE.equals(value.getSinkType())) {
            checkTable(
                    value.getSinkColumns(),
                    value.getSinkTable(),
                    value.getSinkExtend(),
                    value.getSinkPk());
        }

        BroadcastState<String, TableProcess> broadcastState =
                ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getSourceTable() + "-" + value.getOperateType();
        broadcastState.put(key,value);


    }

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {

        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //TODO create table if not exists db.tab（）
    private void checkTable(String sinkColumns, String sinkTable, String sinkExtend, String sinkPk)  {

        try {
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            if (sinkPk == null) {
                sinkPk = "id";
            }
            StringBuffer createTBSQL = new StringBuffer("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] split = sinkColumns.split(",");
            for (int i = 0; i < split.length; i++) {

                String field = split[i];
                if (sinkPk.equals(field)) {
                    createTBSQL.append(field)
                            .append(" varchar primary key ");
                } else {
                    createTBSQL.append(field).append(" varchar ");
                }
                if (i < split.length - 1) {
                    createTBSQL.append(",")
                            .append(sinkExtend);
                }

            }
            createTBSQL.append(")");
            System.out.println(createTBSQL);
            ps = connection.prepareStatement(createTBSQL.toString());
            ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Phoenix表" + sinkTable + "建表失败");
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }

        }

    }


    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> broadcastState =
                ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "-" + value.getString("type");
        //TODO 相当于拿到主流这一条数据的对应的配置数据
        TableProcess tableProcess = broadcastState.get(key);
        if (tableProcess != null) {
            JSONObject after = value.getJSONObject("after");
            filterColumn(after, tableProcess.getSinkColumns());
            value.put("sinkTable", tableProcess.getSinkTable());
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                ctx.output(outputTag, value);
            } else if (
                    TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                out.collect(value);
            }
        }else {
            System.out.println("该组合Key：" + key + "不存在");
        }
    }

    private void filterColumn(JSONObject after, String sinkColumns) {
        String[] field = sinkColumns.split(",");
        List<String> filedLS = Arrays.asList(field);

        Iterator<Map.Entry<String, Object>> iterator = after.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            if (!filedLS.contains(next.getKey())) {
                iterator.remove();
            }
        }
    }
}
