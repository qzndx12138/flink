package day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 刘帅
 * @create 2021-09-08 21:21
 */


public class Flink11_TransForm_Repartition {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStreamSource<String> streamSource = env.socketTextStream("CJhadoop102", 9999);

        SingleOutputStreamOperator<String> map = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).setParallelism(2);

        //KeyBy
        KeyedStream<String, String> keyBy = map.keyBy(r -> r);

        //Shuffle
        DataStream<String> shuffle = map.shuffle();

        //Rebalance
        DataStream<String> rebalance = map.rebalance();

        //Rescale
        DataStream<String> rescale = map.rescale();


        map.print("原始数据").setParallelism(2);
        keyBy.print("keyBy");
        shuffle.print("shuffle");
        rebalance.print("rebalance");
        rescale.print("rescale");

        env.execute();
    }
}
