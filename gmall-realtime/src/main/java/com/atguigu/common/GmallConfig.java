package com.atguigu.common;

public class GmallConfig {

    //TODO Phoenix 库名
    public static final String HBASE_SCHEMA = "GMALL2021_REALTIME";

    //TODO phoenix 驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";


    //TODO phoenix 链接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
}
