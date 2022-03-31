package com.atguigu.realtime.app.dwd;

import com.atguigu.realtime.app.BaseApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/3/31 22:53
 */
public class DwdDbApp extends BaseApp {
    public static void main(String[] args) {
        new DwdDbApp().init(10001, "ods_db", "test01", "dwddbapp");

    }

    @Override
    public void run(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        readProcessTable(env);
    }

    private void readProcessTable(StreamExecutionEnvironment env) {
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE `table_process`( " +
                "   `source_table`  string, " +
                "   `operate_type`  string, " +
                "   `sink_type`  string, " +
                "   `sink_table`  string, " +
                "   `sink_columns` string, " +
                "   `sink_pk`  string, " +
                "   `sink_extend`  string, " +
                "   PRIMARY KEY (`source_table`,`operate_type`)  NOT ENFORCED" +
                ")with(" +
                "   'connector' = 'mysql-cdc', " +
                "   'hostname' = 'hadoop162', " +
                "   'port' = '3306', " +
                "   'username' = 'root', " +
                "   'password' = '123456', " +
                "   'database-name' = 'gmall2021_realtime', " +
                "   'table-name' = 'table_process'," +
                "   'debezium.snapshot.mode' = 'initial' " +  // 每次重启读取mysql的全量,之后增量以及更新数据
                ")"
        );
        tableEnv.sqlQuery("select * from table_process ").execute().print();
    }
}
