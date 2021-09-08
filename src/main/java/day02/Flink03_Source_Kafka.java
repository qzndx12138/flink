package day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author 刘帅
 * @create 2021-09-07 16:38
 */


public class Flink03_Source_Kafka {
    public static void main(String[] args) throws Exception {
        //创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "CJhadoop102:9092,CJhadoop103:9092,CJhadoop104:9092");
        properties.setProperty("group.id", "Flink03_Source_Kafka");
        properties.setProperty("auto.offset.reset", "latest");

        env.addSource(new FlinkKafkaConsumer<>("testTopic",new SimpleStringSchema(),properties)).print("kafka source");

        env.execute();
    }
}
