package day03;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * @author 刘帅
 * @create 2021-09-08 20:28
 */


public class Flink08_TransForm_MaxWithMaxBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("CJhadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> flatMap = streamSource.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });
        //TODO 对相同key的数据进行分组并分区
        KeyedStream<WaterSensor, String> keyedStream = flatMap.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });
        //使用聚和算子 Max求水位的最大值
//        SingleOutputStreamOperator<WaterSensor> result = keyedStream.max("vc");

        //true:每次ts取最新的数据，false：ts取第一个传入的数值
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.maxBy("vc", true);

        result.print();
        env.execute();
    }
}
