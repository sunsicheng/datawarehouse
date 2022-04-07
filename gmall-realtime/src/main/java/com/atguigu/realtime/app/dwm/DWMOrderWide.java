package com.atguigu.realtime.app.dwm;

import com.atguigu.realtime.app.BaseApp;
import com.atguigu.realtime.app.BaseAppV2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        dwd_order_detail.print("detail");
        dwd_order_info.print("info");

    }
}
