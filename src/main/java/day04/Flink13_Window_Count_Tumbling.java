package day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 *      基于元素个数的窗口(按照指定的数据条数生成一个Window，与时间无关)
 * @author 刘帅
 * @create 2021-09-10 21:19
 */


public class Flink13_Window_Count_Tumbling {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("CJhadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = streamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = map.keyBy(0);

        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> countWindow = keyedStream.countWindow(5);
        countWindow.sum(1).print();

        env.execute();
    }
}
