package com.atguigu.realtime.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/4/14 22:16
 */
public class MyThreadUtils {
    public static ThreadPoolExecutor getThread(){
        return new ThreadPoolExecutor(
                2,      //线程池创建初始线程数
                100,  //最大线程数
                300,    //线程
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(100)      //超过线程池能力的时候, 提交的线程会暂存到这个阻塞队列中
        );
    }
}
