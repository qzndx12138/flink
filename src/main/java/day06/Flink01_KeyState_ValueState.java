package day06;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author 刘帅
 * @create 2021-09-13 11:30
 */


public class Flink01_KeyState_ValueState {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("CJhadoop102", 9999);

        //将接收的数据转化为WaterSensor格式
        SingleOutputStreamOperator<WaterSensor> waterSensorStream = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //将相同Key值的数据聚合到一起
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorStream.keyBy("id");

        //检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            //TODO 1、定义状态
            private ValueState<Integer> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //TODO 2、初始化状态
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value_state",Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                // TODO 3、使用状态

                //获取状态
                if (valueState.value() != null && Math.abs(value.getVc()-valueState.value()) > 10){
                    out.collect("相差水位大于10，报警！！！！");
                }

                //更新状态
                valueState.update(value.getVc());
            }
        }).print();

        env.execute();
    }
}
