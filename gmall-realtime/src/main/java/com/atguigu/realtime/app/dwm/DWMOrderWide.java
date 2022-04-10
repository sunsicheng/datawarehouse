package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseApp;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.OrderDetail;
import com.atguigu.realtime.bean.OrderInfo;
import com.atguigu.realtime.bean.OrderWide;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Map;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/4/7 23:03
 */
public class DWMOrderWide  extends BaseAppV2 {

    public static void main(String[] args) {
        new DWMOrderWide().init(30003,1,"testdwmorderwide01","dwmorderwide","dwd_order_info","dwd_order_detail");
    }

    @Override
    public void run(StreamExecutionEnvironment env, Map<String, DataStreamSource<String>> ds) {
        DataStreamSource<String> dwd_order_detail = ds.get("dwd_order_detail");
        DataStreamSource<String> dwd_order_info = ds.get("dwd_order_info");
        KeyedStream<OrderDetail, Long> orderDetailLongKeyedStream = dwd_order_detail.map(new MapFunction<String, OrderDetail>() {
            @Override
            public OrderDetail map(String value) throws Exception {
                return JSONObject.parseObject(value, OrderDetail.class);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                })
        ).keyBy(x -> x.getOrder_id());


        KeyedStream<OrderInfo, Long> orderInfoLongKeyedStream = dwd_order_info.map(new MapFunction<String, OrderInfo>() {
            @Override
            public OrderInfo map(String value) throws Exception {
                return JSONObject.parseObject(value, OrderInfo.class);
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                })
        ).keyBy(x -> x.getId());

        SingleOutputStreamOperator<OrderWide> wideStream = orderInfoLongKeyedStream
                .intervalJoin(orderDetailLongKeyedStream)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo right, OrderDetail left, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(right, left));
                    }
                });


    }
}
