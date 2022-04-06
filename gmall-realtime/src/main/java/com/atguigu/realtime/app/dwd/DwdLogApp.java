package com.atguigu.realtime.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONValidator;
import com.atguigu.realtime.app.BaseApp;
import com.atguigu.realtime.util.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import sun.awt.windows.WPrinterJob;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/3/30 21:45
 */
public class DwdLogApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwdLogApp().init(20001, 3, "ods_log", "test07", "dwdlogapp");
    }

    @Override
    public void run(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        SingleOutputStreamOperator<JSONObject> disDataStream = distinguishCustomer(env, ds);
        Tuple3<SingleOutputStreamOperator<String>, DataStream, DataStream> divideStream = divideStream(disDataStream);
        divideStream.f0.addSink(KafkaUtils.getKafkaSink("dwd_page_log")).setParallelism(1);
        divideStream.f1.addSink(KafkaUtils.getKafkaSink("dwd_start_log")).setParallelism(1);
        divideStream.f2.addSink(KafkaUtils.getKafkaSink("dwd_display_log")).setParallelism(1);
    }

    private Tuple3<SingleOutputStreamOperator<String>, DataStream, DataStream> divideStream(SingleOutputStreamOperator<JSONObject> disDataStream) {
        OutputTag displayTag = new OutputTag<String>("display") {
        };
        OutputTag startTag = new OutputTag<String>("start") {
        };
        SingleOutputStreamOperator<String> pageStream = disDataStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                JSONObject start = value.getJSONObject("start");
                if (start != null) {
                    ctx.output(startTag, value.toJSONString());
                } else {

                    if (value.containsKey("page")) {
                        out.collect(value.toJSONString());
                    }


                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 3) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject dispaly = displays.getJSONObject(i);
                            String page_id = value.getJSONObject("page").getString("page_id");
                            Long ts = value.getLong("ts");
                            dispaly.put("page_id", page_id);
                            dispaly.put("ts", ts);
                            ctx.output(displayTag, dispaly.toJSONString());
                        }
                    }
                }
            }
        });

        DataStream<String> startStream = pageStream.getSideOutput(startTag);
        DataStream<String> displayStream = pageStream.getSideOutput(displayTag);

        return Tuple3.of(pageStream, startStream, displayStream);


    }

    private SingleOutputStreamOperator<JSONObject> distinguishCustomer(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        SingleOutputStreamOperator<JSONObject> disDataStream = ds.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return JSONValidator.from(value).validate();
            }
        }).map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSONObject.parseObject(value);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(((element, recordTimestamp) -> element.getLong("ts"))))
                .keyBy(x -> x.getJSONObject("common").getString("mid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                    private ValueState<Long> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value_state",
                                Long.class));
                    }

                    @Override
                    public void process(String s, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) throws Exception {
                        //如果状态里面没有数据，则说明从没来过，应该标记为新用户
                        if (valueState.value() == null) {
                            List<JSONObject> list = new ArrayList<>();
                            for (JSONObject element : elements) {
                                list.add(element);
                            }
                            list.sort(Comparator.comparing(o -> o.getLong("ts")));

                            for (int i = 0; i < list.size(); i++) {
                                if (i == 0) {
                                    list.get(i).getJSONObject("common").put("is_new", 1);
                                    //更新状态
                                    valueState.update(list.get(i).getLong("ts"));
                                } else {
                                    list.get(i).getJSONObject("common").put("is_new", 0);
                                }
                                out.collect(list.get(i));
                            }

                        } else {
                            elements.forEach(data -> {
                                data.getJSONObject("common").put("is_new", 0);
                                out.collect(data);
                            });
                        }
                    }
                });

        return disDataStream;
    }
}
