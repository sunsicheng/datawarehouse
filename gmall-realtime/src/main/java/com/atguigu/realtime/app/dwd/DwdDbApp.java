package com.atguigu.realtime.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONValidator;
import com.atguigu.realtime.app.BaseApp;
import com.atguigu.realtime.bean.TableProcess;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/3/31 22:53
 */
public class DwdDbApp extends BaseApp {
    public static void main(String[] args) {
        new DwdDbApp().init(20001, 1, "ods_db", "test01", "dwddbapp");
    }

    @Override
    public void run(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //读取配置表
        //SingleOutputStreamOperator<TableProcess> processDs = readProcessTable(env);
        etl(ds).print();

    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> ds) {
        return ds.filter(x -> JSONValidator.from(x).validate())
                .map(x -> JSONObject.parseObject(x))
                .filter(json -> json.getJSONObject("data") != null
                        && json.getString("table") != null
                        && json.getJSONObject("data").size() >= 3);
    }

    private SingleOutputStreamOperator<TableProcess> readProcessTable(StreamExecutionEnvironment env) {
        final StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv
                .executeSql("CREATE TABLE `table_process`( " +
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
                        "   'debezium.snapshot.mode' = 'initial' " +  // 读取mysql的全量,增量以及更新数据
                        ")");


        //TODO 顺序和样例类顺序一致会报错
        Table process = tenv.sqlQuery("select " +
                " source_table  sourceTable, " +
                " sink_type sinkType," +
                " sink_table sinkTable," +
                " sink_columns sinkColumns," +
                " operate_type  operateType ,  " +
                " sink_pk  sinkPk, " +
                " sink_extend sinkExtend " +
                "from table_process ");
        SingleOutputStreamOperator<TableProcess> ds = tenv
                .toRetractStream(process, TableProcess.class)
                .filter(x -> x.f0)
                .map(x -> x.f1);
        return ds;

    }


}
