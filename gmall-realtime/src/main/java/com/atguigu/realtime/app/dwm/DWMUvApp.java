package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONValidator;
import com.atguigu.realtime.app.BaseApp;
import com.atguigu.realtime.util.KafkaUtils;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/4/5 20:44
 */
public class DWMUvApp extends BaseApp {
    public static void main(String[] args) {
        new DWMUvApp().init(30001, 1, "dwd_page_log", "testUV06", "uvapp");
    }

    @Override
    public void run(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        SingleOutputStreamOperator<JSONObject> dwm_uv = ds.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                if (JSONValidator.from(value).validate()) {
                    out.collect(JSONObject.parseObject(value));
                }
            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }))
                .keyBy(x -> x.getJSONObject("common").getString("mid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    private SimpleDateFormat simpleDateFormat;
                    private ValueState<Long> uvstate;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        uvstate = getRuntimeContext().getState(new ValueStateDescriptor<Long>("uvstate", Long.class));
                        //TODO yyyy-mm-dd将会报错
                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public void process(String s,
                                        Context context,
                                        Iterable<JSONObject> elements,
                                        Collector<JSONObject> out) throws Exception {

                        //1.当该mid设备没有数据产生，状态为空
                        //2.当该mid设备有数据产生， 但是到了计算第二天uv的时候（第二天时间戳需要使用水印watermark）
                        String nowWM = simpleDateFormat.format(new Date(context.currentWatermark()));
                        if (uvstate.value() == null
                                || !nowWM.equals(simpleDateFormat.format(new Date(uvstate.value())))) {
                            //使用com.google.guava包，将Iterable直接转换成list
                            ArrayList<JSONObject> list = Lists.newArrayList(elements);
                            JSONObject min = Collections.min(list, Comparator.comparing(o -> o.getLong("ts")));

                            //  优化1 当到了第二天凌晨watermark更新，但是昨天最后一个窗口还未关闭，最后那个窗口将重复写出
                            //  优化2 防止第一次登录时间为当天最后一个窗口，数据不写入
                            if (nowWM.equals(simpleDateFormat.format(new Date(uvstate
                                    .value()))) || uvstate.value() == null) {
                                out.collect(min);
                                uvstate.update(min.getLong("ts"));
                            }

                        }
                    }
                });

        dwm_uv
                .map(x->x.toJSONString())
                .addSink(KafkaUtils.getKafkaSink("dwm_uv"));


    }
}
