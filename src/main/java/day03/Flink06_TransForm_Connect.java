package day03;

import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author 刘帅
 * @create 2021-09-08 19:50
 */


public class Flink06_TransForm_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //获取数据
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5, 6);

        DataStreamSource<String> source2 = env.fromElements("a", "b", "c", "d", "e");

        ConnectedStreams<Integer, String> connect = source1.connect(source2);
    
        //TODO 将两个流连接到一起（但是互不干扰）
        SingleOutputStreamOperator<String> process = connect.process(new CoProcessFunction<Integer, String, String>() {
            @Override
            public void processElement1(Integer integer, Context context, Collector<String> collector) throws Exception {
                collector.collect("10" + integer);
            }

            @Override
            public void processElement2(String s, Context context, Collector<String> collector) throws Exception {
                collector.collect("10" + s);
            }
        });

        process.print();
        env.execute();



    }
}
