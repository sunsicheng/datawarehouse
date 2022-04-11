package com.atguigu.realtime.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/4/11 21:51
 */
public class JedisUtils {

    private static  JedisPool jedisPool;

    public  static Jedis getRedisClient() {

        if (jedisPool == null) {                //提高效率，如果redis不为空才进行抢锁
            synchronized (JedisUtils.class) {
                if (jedisPool == null) {        //多线程安全问题
                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxIdle(5);    //最大闲置连接数
                    config.setMaxTotal(10);     //最大可用连接数
                    config.setMaxWaitMillis(10);   //等待时间
                    config.setTestOnBorrow(true);   //ping pong  测试
                    config.setBlockWhenExhausted(true); //连接耗尽是否等待
                    jedisPool = new JedisPool(config, "hadoop162");
                    return jedisPool.getResource();
                }
            }
        }
        return jedisPool.getResource();
    }
}
