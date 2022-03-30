package com.atguigu.realtime.app.dwd;

import com.atguigu.realtime.app.BaseApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/3/30 21:45
 */
public class DwdLogApp extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwdLogApp().init(1,"ods_log","dwdlogapp");
    }

    @Override
    public void run(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        ds.print();
        //TODO 1.纠正新老客户 2.将启动日志，曝光日志，页面日志分离
    }
}
