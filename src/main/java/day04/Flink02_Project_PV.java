package day04;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 刘帅
 * @create 2021-09-10 9:11
 */


public class Flink02_Project_PV {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //将数据转换为JavaBean
        SingleOutputStreamOperator<UserBehavior> map = streamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");

                UserBehavior userBehavior = new UserBehavior(Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4]));

                return userBehavior;
            }
        });

        //过滤出pv行为的数据
        SingleOutputStreamOperator<UserBehavior> filter = map.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });

        //将数据组成tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> pvToOneStream = filter.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return Tuple2.of(value.getBehavior(), 1);
            }
        });

        //对数据进行key操作
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = pvToOneStream.keyBy(0);

        //累加计算
        keyedStream.sum(1).print();


        env.execute();
    }
}
