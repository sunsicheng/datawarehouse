package com.atguigu.realtime.util;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.realtime.bean.ProductStats;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/5/2 20:09
 */
public abstract class DimAsyncJoinUtils<T> extends RichAsyncFunction<T, T> {
    private String phoenixURL = "jdbc:phoenix:hadoop162,hadoop163,hadoop164:2181";
    private Connection conn;
    private ThreadPoolExecutor pool;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection(phoenixURL);
        pool = MyThreadUtils.getThread();
    }

    @Override
    public void asyncInvoke(T input,
                            ResultFuture<T> resultFuture) throws Exception {

        pool.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    Jedis redisClient = JedisUtils.getRedisClient();
                    joinDim(input,redisClient);
                    redisClient.close();
                    resultFuture.complete(Collections.singletonList(input));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });


    }


    public JSONObject readDim(Jedis redisClient, String tableName, Object id) throws InstantiationException, IllegalAccessException {

        // 先从缓存读数据, 缓存没有, 再从HBase读数据
        String key = tableName + ":" + id;
        if (redisClient.exists(key)) {
            //System.out.println(tableName + "的 id " + id + " 走缓存");
            String userInfoJson = redisClient.get(key);
            return JSON.parseObject(userInfoJson);
        } else {
            //System.out.println(tableName + "的 id " + id + " 走数据库");
            String sql = "select * from " + tableName + " where ID = ?";
            JSONObject jsonObj = JdbcUtils
                    .queryList(conn, sql, new Object[]{id}, JSONObject.class, false)
                    .get(0);
            // 把数据存入缓存, 过期时间一周
            redisClient.setex(key, 60 * 60 * 24 * 7, jsonObj.toJSONString());
            return jsonObj;
        }
    }

    public abstract void joinDim(T input, Jedis redisClient) throws IllegalAccessException, InstantiationException;


    @Override
    public void close() throws Exception {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }


        if (pool!=null && !pool.isShutdown()) {
            pool.shutdown();
        }
    }
}
