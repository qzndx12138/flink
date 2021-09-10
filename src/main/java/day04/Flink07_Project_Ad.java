package day04;

import bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *   各省份页面广告点击量实时统计
 * @author 刘帅
 * @create 2021-09-10 18:59
 */


public class Flink07_Project_Ad {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.获取数据源
        DataStreamSource<String> streamSource = env.readTextFile("input/AdClickLog.csv");

        //3.将数据转为JavaBean
        SingleOutputStreamOperator<AdsClickLog> adsClickLogSingleOutputStreamOperator = streamSource.map(new MapFunction<String, AdsClickLog>() {
            @Override
            public AdsClickLog map(String value) throws Exception {
                String[] split = value.split(",");
                return new AdsClickLog(Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        split[2],
                        split[3],
                        Long.parseLong(split[4]));
            }
        });


        //4.将数据再转为tuple元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = adsClickLogSingleOutputStreamOperator.map(new MapFunction<AdsClickLog, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(AdsClickLog value) throws Exception {
                return Tuple2.of(value.getProvince() + "_" + value.getAdId(), 1);
            }
        });

        //5.分组计算
        map.keyBy(0).sum(1).print();

        env.execute();
    }
}
