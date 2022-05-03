package com.atguigu.realtime.app.dws;

import com.atguigu.realtime.app.BaseSqlApp;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/5/3 15:35
 */
public class DWSProvinceStatsSqlApp extends BaseSqlApp {
    public static void main(String[] args) {
        new DWSProvinceStatsSqlApp().init(40003, 2, "DWSProvinceStatsSqlApp");
    }

    @Override
    public void run(StreamTableEnvironment tenv) {
        //1. 创建表跟数据源相连
        tenv.executeSql("CREATE TABLE orderWide ( \n" +
                "\tprovince_id INTEGER,  \n" +
                "\tprovince_name STRING,  \n" +
                "\tprovince_area_code STRING,  \n" +
                "\tprovince_iso_code STRING,  \n" +
                "\tprovince_3166_2_code STRING,  \n" +
                "\torder_id INTEGER,  \n" +
                "\tsplit_total_amount DECIMAL(12, 2),  \n" +
                "\tcreate_time STRING,  \n" +
                "\tevent_time as TO_TIMESTAMP(create_time) ,  \n" +
                "\tWATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND   \n" +         //水印
                ") WITH (  \n" +
                "\t'connector' = 'kafka',  \n" +
                "\t'topic' = 'dwm_orderwide',  \n" +
                "\t'properties.bootstrap.servers' = 'hadoop162:9092,hadoop163:9092,hadoop164:9092',  \n" +
                "\t'properties.group.id' = 'DWSProvinceStatsSqlApp',  \n" +
                "\t'scan.startup.mode' = 'latest-offset',  \n" +
                "\t'format' = 'json'  \n" +
                ")");
        //2. 创建表跟sink相连
        tenv.executeSql("create table province_stats_2021(" +
                "   stt string," +
                "   edt string," +
                "   province_id bigint," +
                "   province_name string," +
                "   area_code string," +
                "   iso_code string," +
                "   iso_3166_2 string," +
                "   order_amount decimal(20, 2)," +
                "   order_count bigint," +
                "   ts bigint, " +
                "   primary key(stt, edt, province_id) not enforced " +
                ")with(" +
                "   'connector' = 'clickhouse',  " +            //固定写法，clickhouse
                "   'url' = 'clickhouse://hadoop162:8123',  " +
                "   'database-name' = 'gmall2021', " +
                "   'table-name' = 'province_stats_2021'," +
                "   'sink.batch-size' = '2', " +              //数据达到2条就往外写  满足其一即可
                "   'sink.flush-interval' = '2000', " +        //每2秒写一次
                "   'sink.max-retries' = '3' " +
                ")");

        //3. 从源表查询数据插入到sink表
        Table table = tenv.sqlQuery(" select \n" +
                " date_format(tumble_start(event_time,interval '10' second),'yyyy-MM-dd HH:mm:ss') as w_start ,\n" +
                " date_format(tumble_end(event_time,interval '10' second), 'yyyy-MM-dd HH:mm:ss') as w_end ,\n" +
                " province_id ,  \n" +
                " province_name ,  \n" +
                " province_area_code ,  \n" +
                " province_iso_code ,  \n" +
                " province_3166_2_code , \n" +
                " sum(split_total_amount) as order_amount,\n" +
                " count(distinct(order_id)) as order_count,\n" +
                " UNIX_TIMESTAMP()*1000 as ts\n" +
                " from orderWide\n" +
                " group by\n" +
                " tumble(event_time,interval '10' second), \n" +        //窗口
                " province_id ,  \n" +
                " province_name ,  \n" +
                " province_area_code ,  \n" +
                " province_iso_code ,  \n" +
                " province_3166_2_code ");    //语句最后不用加分号;


        table.executeInsert("province_stats_2021");

    }
}
