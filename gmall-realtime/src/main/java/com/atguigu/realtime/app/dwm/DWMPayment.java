package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONValidator;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.OrderWide;
import com.atguigu.realtime.bean.PaymentInfo;
import com.atguigu.realtime.bean.PaymentWide;
import com.atguigu.realtime.util.KafkaUtils;
import com.atguigu.realtime.util.MyTimeUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.reflect.api.Trees;

import java.time.Duration;
import java.util.Map;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/4/16 10:53
 */
public class DWMPayment extends BaseAppV2 {
    public static void main(String[] args) {
        new DWMPayment().init(30007,
                2,
                "DWMPayment",
                "DWMPayment",
                "dwm_orderwide",
                "dwd_payment_info"
        );
    }

    @Override
    public void run(StreamExecutionEnvironment env, Map<String, DataStreamSource<String>> ds) {

        KeyedStream<PaymentInfo, Long> paymentStream = ds
                .get("dwd_payment_info")
                .flatMap(new FlatMapFunction<String, PaymentInfo>() {
                    @Override
                    public void flatMap(String value, Collector<PaymentInfo> out) throws Exception {
                        if (JSONValidator.from(value).validate()) {
                            out.collect(JSONObject.parseObject(value, PaymentInfo.class));
                        }
                    }
                }).assignTimestampsAndWatermarks(WatermarkStrategy
                        .<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                return MyTimeUtils.toTs(element.getCreate_time(), "yyyy-MM-dd HH:mm:ss");
                            }
                        })
                ).keyBy(x -> x.getOrder_id());


        KeyedStream<OrderWide, Long> orderwideStream = ds.get("dwm_orderwide")
                .flatMap(new FlatMapFunction<String, OrderWide>() {
                    @Override
                    public void flatMap(String value, Collector<OrderWide> out) throws Exception {
                        if (JSONValidator.from(value).validate()) {
                            out.collect(JSONObject.parseObject(value, OrderWide.class));
                        }
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                return MyTimeUtils.toTs(element.getCreate_time(), "yyyy-MM-dd HH:mm:ss");
                            }
                        })
                ).keyBy(x -> x.getOrder_id());


        SingleOutputStreamOperator<PaymentWide> paymentWideStream = paymentStream.intervalJoin(orderwideStream)
                .between(Time.milliseconds(-30), Time.seconds(10))           //第一个时间一定要是负数
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });

        paymentWideStream.print();
        paymentWideStream
                .map(x-> JSON.toJSONString(x))
                .addSink(KafkaUtils.getKafkaSink("dwm_paymentWide"));

    }
}
