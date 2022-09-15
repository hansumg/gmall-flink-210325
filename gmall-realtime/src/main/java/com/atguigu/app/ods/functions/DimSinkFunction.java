package com.atguigu.app.ods.functions;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {


    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);

    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement ps = null;
        try {
            String upsertSql = creatSql(value);
            System.out.println(upsertSql);
            ps = connection.prepareStatement(upsertSql);
            ps.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                ps.close();
            }
        }

    }

    private String creatSql(JSONObject value) {
        String sinkTable = value.getString("sinkTable");
        JSONObject after = value.getJSONObject("after");
        Set<String> keys = after.keySet();
        Collection<Object> values = after.values();
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" + StringUtils.join(keys, ",") + ") values('" + StringUtils.join(values, "','") + "')";
    }
}
