package com.atguigu.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.OrderDetail;
import com.atguigu.realtime.bean.OrderInfo;
import com.atguigu.realtime.bean.OrderWide;
import com.atguigu.realtime.util.JdbcUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Map;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/4/7 23:03
 */
public class DWMOrderWide extends BaseAppV2 {

    public static void main(String[] args) {
        new DWMOrderWide().init(30003, 1, "testdwmorderwide01", "dwmorderwide", "dwd_order_info", "dwd_order_detail");
    }

    @Override
    public void run(StreamExecutionEnvironment env, Map<String, DataStreamSource<String>> ds) {

        DataStreamSource<String> dwd_order_detail = ds.get("dwd_order_detail");
        DataStreamSource<String> dwd_order_info = ds.get("dwd_order_info");
        SingleOutputStreamOperator<OrderWide> wideStream = factStreamJoin(dwd_order_info, dwd_order_detail);
        //wideStream.print();
        joinDim(wideStream).print();


    }

    /***
     * join维度信息
     * @param wideStream
     * @return
     */
    private SingleOutputStreamOperator<OrderWide> joinDim(SingleOutputStreamOperator<OrderWide> wideStream) {
        SingleOutputStreamOperator<OrderWide> orderWideStreamDim =
                wideStream.map(new RichMapFunction<OrderWide, OrderWide>() {

                    private Connection connection;
                    String url = "jdbc:phoenix:hadoop162,hadoop163,hadoop164:2181";

                    public JSONObject query(String tableName, Object[] args) throws InstantiationException, IllegalAccessException {
                        String sql = "select * from " + tableName + " where ID=?  ";
                        JSONObject jsonObject = JdbcUtils.queryList(connection, sql, args, JSONObject.class, false).get(0);
                        return jsonObject;
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        connection = DriverManager.getConnection(url);
                    }

                    @Override
                    public OrderWide map(OrderWide orderWide) throws Exception {
                        //查询hbase用户表的信息
                        JSONObject dim_user_info = query("DIM_USER_INFO", new Object[]{orderWide.getUser_id().toString()});
                        orderWide.setUser_gender(dim_user_info.getString("GENDER"));
                        orderWide.calcUserAgeByBirthday(dim_user_info.getString("BIRTHDAY"));

                        //查询province 信息
                        JSONObject dim_base_province = query("DIM_BASE_PROVINCE", new Object[]{orderWide.getProvince_id().toString()});
                        orderWide.setProvince_name(dim_base_province.getString("NAME"));
                        orderWide.setProvince_area_code(dim_base_province.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dim_base_province.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dim_base_province.getString("ISO_3166_2"));

                        //查询sku信息 iphone 8 plus 黑色 128G
                        JSONObject dim_sku_info = query("DIM_SKU_INFO", new Object[]{orderWide.getSku_id().toString()});
                        orderWide.setSku_name(dim_sku_info.getString("SKU_NAME"));
                        orderWide.setSpu_id(dim_sku_info.getLong("SPU_ID"));

                        //join spu信息  iphone 8 plus
                        JSONObject dim_spu_info = query("DIM_SPU_INFO", new Object[]{orderWide.getSpu_id().toString()});
                        orderWide.setSpu_name(dim_spu_info.getString("SPU_NAME"));
                        orderWide.setCategory3_id(dim_spu_info.getLong("CATEGORY3_ID"));
                        orderWide.setTm_id(dim_spu_info.getLong("TM_ID"));

                        //join category3 类别信息
                        JSONObject dim_base_category3 = query("DIM_BASE_CATEGORY3",
                                new Object[]{orderWide.getCategory3_id().toString()});
                        orderWide.setCategory3_name(dim_base_category3.getString("NAME"));

                        //join trademark 品牌信息
                        JSONObject dim_base_trademark = query("DIM_BASE_TRADEMARK", new Object[]{orderWide.getTm_id().toString()});
                        orderWide.setTm_name(dim_base_trademark.getString("TM_NAME"));

                        return orderWide;

                    }


                    @Override
                    public void close() throws Exception {
                        if (connection != null) {
                            connection.close();
                        }
                    }
                });

        return orderWideStreamDim;
    }


    /***
     *   事实表关联
     * @param dwd_order_info
     * @param dwd_order_detail
     * @return
     */
    private SingleOutputStreamOperator<OrderWide> factStreamJoin(DataStreamSource<String> dwd_order_info, DataStreamSource<String> dwd_order_detail) {
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

        return wideStream;
    }
}
