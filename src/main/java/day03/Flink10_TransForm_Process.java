package day03;

import bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @author 刘帅
 * @create 2021-09-08 21:10
 */


public class Flink10_TransForm_Process {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(2);

        //数据
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        //process在keyby之前
        env
                .fromCollection(waterSensors)
                .process(new ProcessFunction<WaterSensor, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(new Tuple2<>(value.getId(), value.getVc()));
                    }
                })
                .print();

        //process在keyby之后:同样的key在同一个分区中
        env
                .fromCollection(waterSensors)
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(new Tuple2<>("key是:" + ctx.getCurrentKey(), value.getVc()));
                    }
                })
                .print();

        env.execute();
    }
}
