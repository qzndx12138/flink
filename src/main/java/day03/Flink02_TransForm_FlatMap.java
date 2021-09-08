package day03;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 刘帅
 * @create 2021-09-08 11:42
 */


public class Flink02_TransForm_FlatMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        DataStreamSource<String> streamSource = env.fromElements("s1_1", "s2_2", "s3_3","s4_4");

        SingleOutputStreamOperator<String> result = streamSource.flatMap(new MyRichFun());

        result.print();

        env.execute();
    }

    public static class MyRichFun extends RichFlatMapFunction<String,String>{
        @Override
        public void open(Configuration parameters) throws Exception {
            int i = 1;
            System.out.println("open第" + i + "次");
  i++;
        }

        @Override
        public void close() throws Exception {
            int i = 1;
            System.out.println("close第" + i + "次");
            i++;
        }

        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            String[] s1 = s.split("_");
            for (String s2 : s1) {
                collector.collect(s2);
            }
        }
    }
}
