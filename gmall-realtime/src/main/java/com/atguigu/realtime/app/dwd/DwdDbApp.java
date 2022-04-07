package com.atguigu.realtime.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONValidator;
import com.atguigu.realtime.app.BaseApp;
import com.atguigu.realtime.bean.TableProcess;
import com.atguigu.realtime.util.KafkaUtils;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/3/31 22:53
 */
public class DwdDbApp extends BaseApp implements Serializable {
    OutputTag<Tuple2<JSONObject, TableProcess>> hbaseTag = new OutputTag<Tuple2<JSONObject, TableProcess>>("hbaseTag") {
    };
    JSONObject jsonObject = new JSONObject();

    public static void main(String[] args) {
        new DwdDbApp().init(20001, 1, "ods_db", "testdwddbapp03", "dwddbapp");
    }

    @Override
    public void run(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //排查问题时，可以将操作链关闭，能够排查到具体哪个算子出问题
        //env.disableOperatorChaining();
        //读取配置表
        SingleOutputStreamOperator<TableProcess> processStream = readProcessTable(env);

        //读取业务数据，简单清洗
        SingleOutputStreamOperator<JSONObject> etlStream = etl(ds);

        //分流。
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dynamicSplitStream = dynamicSplit(processStream, etlStream);

        //数据写入kafka,dwd层
        dynamicSplitStream.addSink(KafkaUtils.kafkaSinkAuto());
        //数据写入dim层
        sink2Hbase(dynamicSplitStream.getSideOutput(hbaseTag));

    }

    private void sink2Hbase(DataStream<Tuple2<JSONObject, TableProcess>> stream) {


        String phoenixUrl = "jdbc:phoenix:hadoop162,hadoop163,hadoop164:2181";
        // 1. 先在hbase中建表

        stream.
                keyBy(x -> x.f1.getSinkTable())     //keyBy保证发往同一个hbase表的数据到同一个task
                .addSink(new RichSinkFunction<Tuple2<JSONObject, TableProcess>>() {

                    private ValueState<Boolean> flag;
                    private Connection connection;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //创建phoenix连接
                        connection = DriverManager.getConnection(phoenixUrl);
                        flag = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("flag", Boolean.class));

                    }

                    @Override
                    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
                        checkTableIfExists(value);

                        write2Hbase(value);
                    }

                    private void write2Hbase(Tuple2<JSONObject, TableProcess> value) throws SQLException {
                        JSONObject data = value.f0.getJSONObject("data");
                        TableProcess tableProcess = value.f1;
                        //upsert into test.Person (IDCardNum,Name,Age) values (100,'小明',12);
                        StringBuilder sql = new StringBuilder();
                        sql.append("upsert into ")
                                .append(tableProcess.getSinkTable())
                                .append("( ")
                                .append(tableProcess.getSinkColumns())
                                .append(") values ( ");

                        //通过列名取出需要插入列的值
                        for (String column : tableProcess.getSinkColumns().split(",")) {
                            sql.append("'").append(data.getString(column)).append("'").append(",");
                        }

                        sql.deleteCharAt(sql.length() - 1);
                        sql.append(")");

                        PreparedStatement ps = connection.prepareStatement(sql.toString());
                        ps.execute();
                        //ps.addBatch();    可使用批处理提高效率
                        connection.commit();
                        ps.close();

                    }

                    /**
                     * 检查hbase里面是否存在表，如果不存在就创建。
                     * CREATE TABLE IF NOT EXISTS "table_2" (
                     *       state CHAR(2) NOT NULL,
                     *       city VARCHAR NOT NULL,
                     *       population BIGINT
                     *       CONSTRAINT my_pk PRIMARY KEY (state, city)
                     * );
                     */
                    private void checkTableIfExists(Tuple2<JSONObject, TableProcess> value) throws IOException, SQLException {
                        TableProcess tableProcess = value.f1;
                        if (flag.value() == null) {
                            StringBuilder createSql = new StringBuilder();
                            createSql.append("CREATE TABLE IF NOT EXISTS ");
                            createSql.append(tableProcess.getSinkTable());
                            createSql.append("(");
                            for (String column : tableProcess.getSinkColumns().split(",")) {
                                createSql.append(column).append(" varchar, ");
                            }
                            createSql.append("CONSTRAINT my_pk PRIMARY KEY (");
                            createSql.append(tableProcess.getSinkPk() == null ? "id" : tableProcess.getSinkPk());
                            createSql.append(")");
                            createSql.append(")");
                            createSql.append(tableProcess.getSinkExtend() == null ? "" : tableProcess.getSinkExtend());


                            PreparedStatement ps = connection.prepareStatement(createSql.toString());
                            ps.execute();
                            connection.commit();
                            ps.close();
                            //更新状态
                            flag.update(true);
                        }
                    }

                    @Override
                    public void close() throws Exception {
                        if (connection != null) {
                            connection.close();
                        }
                    }
                });


    }

    /***
     * 将配置流做成广播状态，在业务数据流中一一判断
     * @param processStream  配置流
     * @param etlStream     业务数据流
     * @return
     */
    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dynamicSplit(SingleOutputStreamOperator<TableProcess> processStream,
                                                                                      SingleOutputStreamOperator<JSONObject> etlStream) {


        MapStateDescriptor<String, TableProcess> processMapStateDescriptor = new MapStateDescriptor<>("processMapStateDescriptor", String.class, TableProcess.class);
        BroadcastStream<TableProcess> processBroadcastStream = processStream.broadcast(processMapStateDescriptor);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> splitStream = etlStream
                .connect(processBroadcastStream)
                .process(new BroadcastProcessFunction<JSONObject, TableProcess, Tuple2<JSONObject, TableProcess>>() {
                    @Override
                    public void processElement(JSONObject value,
                                               ReadOnlyContext ctx,
                                               Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {

                        //1.获取配置流，根据配置流将数据保存到hbase或kafka
                        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(processMapStateDescriptor);
                        //maxwell 初始化数据，类型为 "type":"bootstrap-start"
                        String key = value.getString("table") + "_" + value.getString("type").replaceAll("bootstrap-",
                                "");
                        if (broadcastState.contains(key)) {


                            TableProcess tableProcess =
                                    broadcastState.get(value.getString("table") + "_" + value.getString("type").replaceAll("bootstrap-", ""));
                            //System.out.println("key="+key+","+tableProcess);

                            //2.获取保存类型,为hbase 或者kafka
                            String sinkType = tableProcess.getSinkType();


                            //3.过滤出指定需要保存的字段
                            JSONObject data = value.getJSONObject("data");
                            String[] sinkColumns = tableProcess.getSinkColumns().split(",");
                            for (int i = 0; i < sinkColumns.length; i++) {
                                if (jsonObject.containsKey(sinkColumns[i])) {
                                    jsonObject.put(sinkColumns[i], data.get(sinkColumns[i]));
                                }
                            }
                            if (TableProcess.SINK_TYPE_KAFKA.equalsIgnoreCase(sinkType)) {
                                //业务数据发往kafka
                                out.collect(Tuple2.of(data, tableProcess));
                            } else if (TableProcess.SINK_TYPE_HBASE.equalsIgnoreCase(sinkType)) {
                                //配置数据发往hbase测输出流
                                ctx.output(hbaseTag, Tuple2.of(value, tableProcess));
                            } else {

                            }
                        }
                        //清空数据，重复使用
                        jsonObject.clear();
                    }


                    @Override
                    public void processBroadcastElement(TableProcess value,
                                                        Context ctx,
                                                        Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        //1.获取广播状态
                        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(processMapStateDescriptor);
                        //2.将配置数据更新到广播状态,key为source_table和operate_type共同确定
                        //System.out.println(value.getSourceTable() + "_" + value.getOperateType());
                        broadcastState.put(value.getSourceTable() + "_" + value.getOperateType(), value);

                    }
                });

        return splitStream;


    }

    /***
     * maxwell 初始化数据,有开始和结束打印日志，将data为空的过滤
     * {"database":"gmall2021","table":"activity_info","type":"bootstrap-start","ts":1648825463,"data":{}}
     * {"database":"gmall2021","table":"activity_info","type":"bootstrap-complete","ts":1648825463,"data":{}}
     * @param ds
     * @return
     */
    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> ds) {
        //maxwell 初始化数据，{"database":"gmall2021","table":"activity_info","type":"bootstrap-start","ts":1648825463,"data":{}}

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
