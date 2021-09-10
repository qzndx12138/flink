package day04;

import bean.OrderEvent;
import bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 *   订单支付实时监控
 * @author 刘帅
 * @create 2021-09-10 19:09
 */


public class Flink08_Project_Order {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件中读取
        DataStreamSource<String> orderStream = env.readTextFile("input/OrderLog.csv");

        DataStreamSource<String> receiptStream = env.readTextFile("input/ReceiptLog.csv");

        //3.将两条流转为JavaBean
        SingleOutputStreamOperator<OrderEvent> orderEventStream = orderStream.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
            }
        });

        SingleOutputStreamOperator<TxEvent> txEvent = receiptStream.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(
                        split[0],
                        split[1],
                        Long.parseLong(split[2])
                );
            }
        });

        //4.将两条流连接到一块
        ConnectedStreams<OrderEvent, TxEvent> connect = orderEventStream.connect(txEvent);

        //5.将相同交易码的数据聚和到一块
        ConnectedStreams<OrderEvent, TxEvent> orderEventTxEventConnectedStreams = connect.keyBy("txId", "txId");

        //6.实时对账两条流的数据
        orderEventTxEventConnectedStreams.process(new KeyedCoProcessFunction<String, OrderEvent,TxEvent,  String>() {
            //创建Map集合用来缓存OrderEvent数据
            HashMap<String, OrderEvent> orderMap = new HashMap<>();

            //创建Map集合用来缓存TxEvent数据
            HashMap<String, TxEvent> txMap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                if (txMap.containsKey(value.getTxId())){
                    //有能够关联上的数据
                    out.collect("订单:" + value.getOrderId() + "对账成功");
                    //删除能够关联上的数据
                    txMap.remove(value.getTxId());
                } else {
                    //没有能关联上的数据，要把自己存入自己的缓存中
                    orderMap.put(value.getTxId(), value);
                }
            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                if (orderMap.containsKey(value.getTxId())) {
                    //有能够关联上的数据
                    out.collect("订单:" + orderMap.get(value.getTxId()).getOrderId() + "对账成功");
                    //删除能够关联上的数据
                    orderMap.remove(value.getTxId());
                } else {
                    //没有能关联上的数据，要把自己存入自己的缓存中
                    txMap.put(value.getTxId(), value);
                }
            }
        }).print();

        env.execute();
    }
}
