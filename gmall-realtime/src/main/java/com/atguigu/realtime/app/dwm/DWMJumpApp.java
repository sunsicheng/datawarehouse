package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONValidator;
import com.atguigu.realtime.app.BaseApp;
import com.atguigu.realtime.util.KafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;


/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/4/7 21:48
 */
public class DWMJumpApp extends BaseApp {

    public static void main(String[] args) {
        new DWMJumpApp().init(30002, 2, "dwd_page_log", "testjumpapp01", "jumpapp");
    }

    /***
     * 跳出，点击某个入口页面后（如首页），之后在一定时间如10秒内，没有访问其他页面
     * @param env
     * @param ds
     */
    @Override
    public void run(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        KeyedStream<JSONObject, String> keyDS = ds.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                if (JSONValidator.from(value).validate()) {
                    out.collect(JSONObject.parseObject(value));
                }
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                })
        ).keyBy(x -> x.getJSONObject("common").getString("mid"));

        //创建CEP 规则
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                //上一个页面为空
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("second").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") != null;
            }
        }).within(Time.seconds(10));

        //使用规则
        OutputTag<JSONObject> lateTag = new OutputTag<JSONObject>("late") {};
        SingleOutputStreamOperator<Object> stream = CEP.pattern(keyDS, pattern).flatSelect(
                lateTag,
                new PatternFlatTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> map, long l, Collector<JSONObject> collector) throws Exception {
                        //迟到数据自动输出到测输出流
                        List<JSONObject> list = map.get("first");
                        for (JSONObject jsonObject : list) {
                            collector.collect(jsonObject);
                        }

                    }
                },
                new PatternFlatSelectFunction<JSONObject, Object>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<Object> collector) throws Exception {
                        //正常数据，本需求只要迟到数据，故不做处理  map的key为模式id,如first，second
                    }
                }

        );

        stream.getSideOutput(lateTag).print();
        stream.getSideOutput(lateTag).map(x->x.toJSONString()).addSink(KafkaUtils.getKafkaSink("dwm_user_jump_detail")).setParallelism(1);
    }
}
