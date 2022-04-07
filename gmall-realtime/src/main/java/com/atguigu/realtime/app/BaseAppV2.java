package com.atguigu.realtime.app;

import com.atguigu.realtime.util.KafkaUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/3/30 21:18
 */
public abstract class BaseAppV2 {
    public void init(int port ,int parallelism, String groupId,String ck,String ... topics) {
        //设置操作用户，操作HDFS
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        //ck模式为精准一次
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig config = env.getCheckpointConfig();
        //ck超时时间
        config.setCheckpointTimeout(60000);
        //取消任务后保留ck
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop162:8020/gmall2021/flink/checkpoint/" + ck));


        Map<String, DataStreamSource<String>> streams = new HashMap<>();
        for (String topic : topics) {
            streams.put(topic,env.addSource(KafkaUtils.getKafkaConsume(topic, groupId)));
        }

        //主要的处理逻辑
        run(env, streams);


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public abstract void run(StreamExecutionEnvironment env, Map<String, DataStreamSource<String>> ds);
}
