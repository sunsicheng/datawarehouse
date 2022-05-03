package com.atguigu.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.app.BaseAppV2;
import com.atguigu.realtime.bean.OrderWide;
import com.atguigu.realtime.bean.PaymentWide;
import com.atguigu.realtime.bean.ProductStats;
import com.atguigu.realtime.common.GmallConstant;
import com.atguigu.realtime.util.DimAsyncJoinUtils;
import com.atguigu.realtime.util.MyTimeUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/5/2 17:49
 */
public class DWSProductStatsApp extends BaseAppV2 {
    public static void main(String[] args) {
        String[] topis = {
                "dwd_page_log",
                "dwd_display_log",
                "dwd_favor_info",
                "dwd_cart_info",
                "dwm_orderwide",
                "dwm_paymentWide",
                "dwd_refund_payment",
                "dwd_comment_info"
        };
        new DWSProductStatsApp().init(40002, 2, "DWSProductStatsApp", "DWSProductStatsApp", topis);
    }

    @Override
    public void run(StreamExecutionEnvironment env, Map<String, DataStreamSource<String>> ds) {
        DataStream<ProductStats> productStatsDataStream = parseStream(ds);
        SingleOutputStreamOperator<ProductStats> keyProductStatsDataStream = aggregateByDim(productStatsDataStream);
        SingleOutputStreamOperator<ProductStats> streamWithDim = addDims(keyProductStatsDataStream);
        streamWithDim.print();
    }

    private SingleOutputStreamOperator<ProductStats> addDims(SingleOutputStreamOperator<ProductStats> productStatsSingleOutputStreamOperator) {
        SingleOutputStreamOperator<ProductStats> streamWithDim = AsyncDataStream.unorderedWait(
                productStatsSingleOutputStreamOperator,
                new DimAsyncJoinUtils<ProductStats>() {
                    @Override
                    public void joinDim(ProductStats input, Jedis redisClient) throws IllegalAccessException, InstantiationException {
                        //先读取维度
                        // 1. 读取sku_info
                        JSONObject skuInfo = readDim(redisClient, "DIM_SKU_INFO", input.getSku_id().toString());
                        input.setSku_name(skuInfo.getString("SKU_NAME"));
                        input.setSku_price(skuInfo.getBigDecimal("PRICE"));
                        input.setCategory3_id(skuInfo.getLong("CATEGORY3_ID"));
                        input.setSpu_id(skuInfo.getLong("SPU_ID"));
                        input.setTm_id(skuInfo.getLong("TM_ID"));

                        // 2. 读取spu_info
                        final JSONObject spuInfo = readDim(redisClient, "DIM_SPU_INFO", input.getSpu_id().toString());
                        input.setSpu_name(spuInfo.getString("SPU_NAME"));

                        // 3. 读取品牌表
                        final JSONObject tmInfo = readDim(redisClient, "DIM_BASE_TRADEMARK", input.getTm_id().toString());
                        input.setTm_name(tmInfo.getString("TM_NAME"));

                        // 4. 读取三级品类
                        final JSONObject c3Info = readDim(redisClient, "DIM_BASE_CATEGORY3", input.getCategory3_id().toString());
                        input.setCategory3_name(c3Info.getString("NAME"));
                    }
                },
                30,
                TimeUnit.SECONDS
        );
        return streamWithDim;

    }

    private SingleOutputStreamOperator<ProductStats> aggregateByDim(DataStream<ProductStats> productStatsDataStream) {
        KeyedStream<ProductStats, Long> productStatsLongKeyedStream = productStatsDataStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                })
        ).keyBy(x -> x.getSku_id());
       // productStatsDataStream.print("keyed");
       return  productStatsLongKeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats value1, ProductStats value2) throws Exception {
                        value1.setFavor_ct(value1.getFavor_ct() + value2.getFavor_ct());
                        value1.setClick_ct(value1.getClick_ct() + value2.getClick_ct());
                        value1.setDisplay_ct(value1.getDisplay_ct() + value2.getDisplay_ct());
                        value1.setCart_ct(value1.getCart_ct() + value2.getCart_ct());
                        value1.setOrder_amount(value1.getOrder_amount().add(value2.getOrder_amount()));
                        value1.setOrder_sku_num(value1.getOrder_sku_num() + value2.getOrder_sku_num());
                        value1.setPayment_amount(value1.getPayment_amount().add(value2.getPayment_amount()));
                        value1.setRefund_amount(value1.getRefund_amount().add(value2.getRefund_amount() == null ?
                                BigDecimal.ZERO :
                                value2.getRefund_amount()));
                        value1.setComment_ct(value1.getComment_ct() + value2.getComment_ct());
                        value1.setGood_comment_ct(value1.getGood_comment_ct() + value2.getGood_comment_ct());
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        value1.getPaidOrderIdSet().addAll(value2.getPaidOrderIdSet());
                        value1.getRefundOrderIdSet().addAll(value2.getRefundOrderIdSet());

                        return value1;
                    }
                }, new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong,
                                        Context context,
                                        Iterable<ProductStats> elements,
                                        Collector<ProductStats> out) throws Exception {
                        ProductStats productStats = elements.iterator().next();
                        TimeWindow window = context.window();
                        productStats.setStt(MyTimeUtils.toDateTimeString(window.getStart()));
                        productStats.setEdt(MyTimeUtils.toDateTimeString(window.getEnd()));
                        out.collect(productStats);
                    }
                });

    }

    private DataStream<ProductStats> parseStream(Map<String, DataStreamSource<String>> sourceStreams) {
        // 1. 解析得到页面点击流
        final SingleOutputStreamOperator<ProductStats> productClickStream = sourceStreams
                .get("dwd_page_log")
                .flatMap(new FlatMapFunction<String, ProductStats>() {
                    @Override
                    public void flatMap(String json,
                                        Collector<ProductStats> out) throws Exception {
                        final JSONObject obj = JSON.parseObject(json);
                        final JSONObject pageObj = obj.getJSONObject("page");
                        final String pageId = pageObj.getString("page_id");
                        if ("good_detail".equalsIgnoreCase(pageId)) {
                            final Long skuId = pageObj.getLong("item");
                            final Long ts = obj.getLong("ts");

                            final ProductStats ps = new ProductStats();
                            ps.setSku_id(skuId);
                            ps.setClick_ct(1L);
                            ps.setTs(ts);
                            out.collect(ps);
                        }
                    }
                });

        // 2. 曝光率
        final SingleOutputStreamOperator<ProductStats> displayStream = sourceStreams
                .get("dwd_display_log")
                .process(new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String json,
                                               Context ctx,
                                               Collector<ProductStats> out) throws Exception {
                        final JSONObject obj = JSON.parseObject(json);
                        final String itemType = obj.getString("item_type");
                        if ("sku_id".equalsIgnoreCase(itemType)) {
                            final ProductStats ps = new ProductStats();
                            final Long skuId = obj.getLong("item");
                            final Long ts = obj.getLong("ts");
                            ps.setSku_id(skuId);
                            ps.setDisplay_ct(1L);
                            ps.setTs(ts);

                            out.collect(ps);
                        }
                    }
                });

        // 3.收藏流
        final SingleOutputStreamOperator<ProductStats> favorStream = sourceStreams
                .get("dwd_favor_info")
                .map(json -> {
                    final JSONObject obj = JSON.parseObject(json);
                    final Long skuId = obj.getLong("sku_id");
                    final Long ts = MyTimeUtils.toTs(obj.getString("create_time"));

                    final ProductStats ps = new ProductStats();
                    ps.setSku_id(skuId);
                    ps.setTs(ts);
                    ps.setFavor_ct(1L);
                    return ps;
                });
        // 4. 购物车流
        final SingleOutputStreamOperator<ProductStats> cartStream = sourceStreams
                .get("dwd_cart_info")
                .map(json -> {
                    final JSONObject obj = JSON.parseObject(json);
                    final Long skuId = obj.getLong("sku_id");
                    final Long ts = MyTimeUtils.toTs(obj.getString("create_time"));
                    final ProductStats ps = new ProductStats();
                    ps.setSku_id(skuId);
                    ps.setTs(ts);
                    ps.setCart_ct(1L);
                    return ps;
                });
        // 5. 订单流
        final SingleOutputStreamOperator<ProductStats> orderStream = sourceStreams
                .get("dwm_orderwide")
                .map(json -> {
                    final OrderWide orderWide = JSON.parseObject(json, OrderWide.class);

                    final ProductStats ps = new ProductStats();
                    ps.setSku_id(orderWide.getSku_id());
                    ps.setTs(MyTimeUtils.toTs(orderWide.getCreate_time()));
                    ps.getOrderIdSet().add(orderWide.getOrder_id());
                    System.out.println(ps.getOrderIdSet());
                    ps.setOrder_amount(orderWide.getSplit_total_amount());
                    ps.setOrder_sku_num(orderWide.getSku_num());

                    return ps;
                });
        // 6. 支付流
        final SingleOutputStreamOperator<ProductStats> paymentStream = sourceStreams
                .get("dwm_paymentWide")
                .map(json -> {
                    final PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);
                    final ProductStats ps = new ProductStats();
                    ps.setSku_id(paymentWide.getSku_id());
                    ps.setTs(MyTimeUtils.toTs(paymentWide.getPayment_create_time()));
                    ps.getPaidOrderIdSet().add(paymentWide.getOrder_id());
                    ps.setPayment_amount(paymentWide.getSplit_total_amount());

                    return ps;
                });

        // 7. 退款流
        final SingleOutputStreamOperator<ProductStats> refundStream = sourceStreams
                .get("dwd_refund_payment")
                .map(json -> {
                    final JSONObject obj = JSON.parseObject(json);
                    final Long skuId = obj.getLong("sku_id");
                    final Long ts = MyTimeUtils.toTs(obj.getString("create_time"));
                    final BigDecimal refundAmount = obj.getBigDecimal("refund_amount");

                    final ProductStats ps = new ProductStats();
                    ps.setSku_id(skuId);
                    ps.setTs(ts);
                    ps.getRefundOrderIdSet().add(obj.getLong("order_id"));
                    ps.setRefund_amount(refundAmount);

                    return ps;
                });

        // 8. 评论流
        final SingleOutputStreamOperator<ProductStats> commentStream = sourceStreams
                .get("dwd_comment_info")
                .map(json -> {
                    final JSONObject obj = JSON.parseObject(json);
                    final Long skuId = obj.getLong("sku_id");
                    final Long ts = MyTimeUtils.toTs(obj.getString("create_time"));

                    final ProductStats ps = new ProductStats();
                    ps.setSku_id(skuId);
                    ps.setTs(ts);
                    ps.setComment_ct(1L);

                    final String appraise = obj.getString("appraise");
                    if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {  // 硬编码  魔术数字
                        ps.setGood_comment_ct(1L);
                    }

                    return ps;

                });

        return productClickStream.union(
                displayStream,
                favorStream,
                cartStream,
                orderStream,
                paymentStream,
                refundStream,
                commentStream);

    }
}
