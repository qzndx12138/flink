package day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * @author 刘帅
 * @create 2021-09-04 8:59
 */


public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 1、创建执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //使用createLocalEnvironmentWithWebUI(new Configuration())之后，运行程序可以再localhost:8081查看任务执行情况
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);
        // 2、从文件中读取数据，按行读取
//        DataStream<String> inputDataStream = env.readTextFile("input\\word.txt");

        // 2、从socket流中读取数据
        DataStream<String> inputDataStream = env.socketTextStream("CJhadoop102",9999);

        // 3、基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new MyFlatMapper())
                .keyBy(0)
                .sum(1);


        resultStream.print();

        //执行任务
        env.execute();

    }


    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector){
            String[] words = s.split(" ");

            for (String word : words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
