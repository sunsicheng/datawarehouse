package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.VisitorStats;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Map;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/4/18 21:32
 */
public class DWSVisitorStatsApp extends BaseAppV2 {

    public static void main(String[] args) {
        new DWSVisitorStatsApp().init(
                40001,
                1,
                "UvStatus",
                "UvStatus",
                "dwd_page_log",
                "dwm_uv",
                "dwm_user_jump_detail"
        );
    }


    @Override
    public void run(StreamExecutionEnvironment env, Map<String, DataStreamSource<String>> ds) {
        //把三个流处理成一个流
        DataStream<VisitorStats> visitorStatsDataStream = unionStream(ds);
        //对流进行累加
        SingleOutputStreamOperator<VisitorStats> aggrateStream = aggrateStream(visitorStatsDataStream);

         aggrateStream.print();


    }

    private SingleOutputStreamOperator<VisitorStats> aggrateStream(DataStream<VisitorStats> visitorStatsDataStream) {
        SingleOutputStreamOperator<VisitorStats> aggrateStream =
                visitorStatsDataStream.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
                ).keyBy(vs -> vs.getVc() + "_" + vs.getCh() + "_" + vs.getAr() + "_" + vs.getIs_new())
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .reduce(new ReduceFunction<VisitorStats>() {
                                    @Override
                                    public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                                        //将value1和value2的值进行累加，结果返回value1
                                        value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                                        value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                                        value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                                        value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                                        value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                                        return value1;
                                    }
                                },
                                new ProcessWindowFunction<VisitorStats, VisitorStats, String, TimeWindow>() {
                                    private SimpleDateFormat dateFormat;
                                    //每次窗口结束，就会执行第二个函数，第一个参数是上一个窗口的结果


                                    @Override
                                    public void open(Configuration parameters) throws Exception {
                                        dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                    }

                                    @Override
                                    public void process(String key,
                                                        Context context,
                                                        Iterable<VisitorStats> elements,
                                                        Collector<VisitorStats> out) throws Exception {
                                        //补齐窗口开始和结束的信息
                                        VisitorStats visitorStats = elements.iterator().next();
                                        visitorStats.setStt(dateFormat.format(context.window().getStart()));
                                        visitorStats.setEdt(dateFormat.format(context.window().getEnd()));
                                        out.collect(visitorStats);
                                    }
                                }
                        );
        return aggrateStream;


    }

    private DataStream<VisitorStats> unionStream(Map<String, DataStreamSource<String>> ds) {

        //pv数据
        SingleOutputStreamOperator<VisitorStats> pvStream = ds.get("dwd_page_log")
                .map(new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String value) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        JSONObject common = jsonObject.getJSONObject("common");
                        return new VisitorStats(
                                "",
                                "",
                                common.getString("vc"),
                                common.getString("ch"),
                                common.getString("ar"),
                                common.getString("is_new"),
                                0L,
                                1L,
                                0L,
                                0L,
                                0L,
                                jsonObject.getLong("ts"));
                    }
                });

        //uv数据
        SingleOutputStreamOperator<VisitorStats> uvStream = ds.get("dwm_uv")
                .map(new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String value) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        JSONObject common = jsonObject.getJSONObject("common");
                        return new VisitorStats(
                                "",
                                "",
                                common.getString("vc"),
                                common.getString("ch"),
                                common.getString("ar"),
                                common.getString("is_new"),
                                1L,
                                0L,
                                0L,
                                0L,
                                jsonObject.getJSONObject("page").getLong("during_time"),
                                jsonObject.getLong("ts")
                        );
                    }
                });


        //sv 数据 跳入数据
        SingleOutputStreamOperator<VisitorStats> svStream = ds.get("dwd_page_log")
                .flatMap(new FlatMapFunction<String, VisitorStats>() {
                    @Override
                    public void flatMap(String value, Collector<VisitorStats> out) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        JSONObject common = jsonObject.getJSONObject("common");
                        String last_page_id = jsonObject.getJSONObject("page").getString("last_page_id");
                        if (last_page_id == null || last_page_id.length() == 0) {
                            VisitorStats visitorStats = new VisitorStats(
                                    "",
                                    "",
                                    common.getString("vc"),
                                    common.getString("ch"),
                                    common.getString("ar"),
                                    common.getString("is_new"),
                                    0L,
                                    0L,
                                    1L,
                                    0L,
                                    0L,
                                    jsonObject.getLong("ts")
                            );
                        }

                    }
                });


        SingleOutputStreamOperator<VisitorStats> ujStream = ds.get("dwm_user_jump_detail")
                .map(new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String value) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        JSONObject common = jsonObject.getJSONObject("common");
                        return new VisitorStats(
                                "",
                                "",
                                common.getString("vc"),
                                common.getString("ch"),
                                common.getString("ar"),
                                common.getString("is_new"),
                                0L,
                                0L,
                                0L,
                                1L,
                                0L,
                                jsonObject.getLong("ts")
                        );
                    }
                });


        DataStream<VisitorStats> unionStream = pvStream.union(uvStream, svStream, ujStream);
        return unionStream;
    }
}
