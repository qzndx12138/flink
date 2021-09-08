package day03;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Collection;

/**
 * @author 刘帅
 * @create 2021-09-08 18:25
 */


public class Flink04_TransForm_KeyBy {
    public static void main(String[] args) throws Exception {
        //1、获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

//        //2、从元素中获取数据
//        DataStreamSource<String> streamSource = env.socketTextStream("CJhadoop102", 9999);
//
//        SingleOutputStreamOperator<WaterSensor> flatMap = streamSource.flatMap(new FlatMapFunction<String, WaterSensor>() {
//            @Override
//            public void flatMap(String s, Collector<WaterSensor> collector) throws Exception {
//                String[] s1 = s.split("_");
//
//                collector.collect(new WaterSensor(s1[0], Long.parseLong(s1[1]), Integer.parseInt(s1[2])));
//
//            }
//        });
//
//        //3、TODO 对相同的key 进行分组并分区
//        KeyedStream<WaterSensor, String> keyedStream = flatMap.keyBy(new KeySelector<WaterSensor, String>() {
//            @Override
//            public String getKey(WaterSensor waterSensor) throws Exception {
//                return waterSensor.getId();
//            }
//        });
//
//        flatMap.print("原始数据");

        //奇数分一组，偶数分一组（问题：不能显示的表达出来哪一种是奇数，哪一组是偶数）
        env.fromElements(1,2,3,4,5,6,7)
                .keyBy(new KeySelector<Integer, String>() {
                    @Override
                    public String getKey(Integer integer) throws Exception {
                        return integer%2 == 0 ? "偶数":"奇数";
                    }
                }).print();

        env.execute();
    }
}
