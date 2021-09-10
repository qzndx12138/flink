package day04;

import bean.UserBehavior;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author 刘帅
 * @create 2021-09-10 17:56
 */


public class Flink03_Project_PV_Process {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取文件数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //使用Process转化为JavaBean，过滤出pv的数据，转化为Tuple2元组，累加计算
        streamSource.process(new ProcessFunction<String, Tuple2<String,Integer>>() {
            private  Integer count = 0;
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");

                //转化为JavaBean
                UserBehavior userBehavior = new UserBehavior(Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4]));
                //过滤出PV的数据
                if ("pv".equals(userBehavior.getBehavior())){
                    count++;
                    out.collect(Tuple2.of(userBehavior.getBehavior(),count));
                }
            }
        }).print();

        env.execute();
    }
}
