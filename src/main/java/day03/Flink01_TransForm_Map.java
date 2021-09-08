package day03;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author 刘帅
 * @create 2021-09-08 9:41
 */


public class Flink01_TransForm_Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5);

        //利用Filter将偶数过滤掉
        SingleOutputStreamOperator<Integer> filter = streamSource.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer % 2 != 0;
            }
        });
        filter.print();

        env.execute();
    }
}
