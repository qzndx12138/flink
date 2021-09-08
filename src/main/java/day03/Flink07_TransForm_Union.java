package day03;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author 刘帅
 * @create 2021-09-08 20:08
 */


public class Flink07_TransForm_Union {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从元素中获取获取数据

        DataStreamSource<String> source2 = env.fromElements("a", "b", "c", "d", "e");

        DataStreamSource<String> source3 = env.fromElements("1", "2", "3", "4");

        //TODO 3.将两个流或多个流连接到一块（合并为一个流）
        DataStream<String> union = source2.union(source3);

        SingleOutputStreamOperator<String> result = ((DataStream) union).process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                out.collect("10"+value );
            }
        });

union.print();
        result.print();
        env.execute();
    }
}
