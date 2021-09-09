package day03;

import bean.WaterSensor;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * @author 刘帅
 * @create 2021-09-09 11:30
 */


public class Flink01_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("CJhadoop102", 9999);

        SingleOutputStreamOperator<String> map = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] s1 = s.split(" ");
                WaterSensor waterSensor = new WaterSensor(s1[0], Long.parseLong(s1[1]), Integer.parseInt(s1[2]));

                return JSON.toJSONString(waterSensor);
            }
        });

        //将数据发送到kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.server", "CJhadoop102:9092");
        map.addSink(new FlinkKafkaProducer<String>("topicText", new SimpleStringSchema(), properties));

        env.execute ();
    }
}
