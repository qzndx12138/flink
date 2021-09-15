package day06;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @author 刘帅
 * @create 2021-09-13 16:43
 */


public class Flink02_KeyState_ListState {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("CJhadoop102", 9999);

        //转换数据格式为WaterSensor
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //将相同的key聚合
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorSingleOutputStreamOperator.keyBy("id");

        //针对每个传感器输出最高的3个水位值
        
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
            //TODO 定义状态
            private ListState<Integer> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //TODO 初始化状态
                listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("list_state",Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //TODO 使用状态
                listState.add(value.getVc());

                //创建list集合，存放状态中的数据
                ArrayList<Integer> list = new ArrayList<>();

                //获取状态中的数据
                for (Integer integer : listState.get()) {
                    list.add(integer);
                }

                //对list集合中的数据排序
                list.sort((o1, o2) -> o2 - o1);

                //如果list中的数据大于三条则把第四天也就是最小的删除掉
                if (list.size()>3){
                    list.remove(3);
                }
                //将最大的三条数据存放到状态中
                listState.update(list);
                out.collect(list.toString());
            }
        }).print();

        env.execute();
    }
}
