package day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author 刘帅
 * @create 2021-09-08 19:24
 */


public class Flink05_TransForm_Shuffle {
    public static void main(String[] args) throws Exception {
        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("CJhadoop102", 9999);

        SingleOutputStreamOperator<String> streamOperator = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                collector.collect(s);
            }
        });


        //疑问：为什么print函数后加上分区数时，两个输出会反转，会先输出shuffle之后的数据？
        streamOperator.print("原始数据")/*.setParallelism(2)*/;

        streamOperator.shuffle().print("shuffle后的数据")/*.setParallelism(2)*/;

        env.execute();
    }
}
