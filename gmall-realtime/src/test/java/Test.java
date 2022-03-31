import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONValidator;
import com.atguigu.realtime.util.KafkaUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/3/30 23:22
 */
public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer<String> kafkaConsume = KafkaUtils.getKafkaConsume("dwd_start_log", "test01");

        DataStreamSource<String> ds = env.addSource(kafkaConsume);
        ds.print();
        env.execute();
    }
}
