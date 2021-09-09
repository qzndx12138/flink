package day03;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author 刘帅
 * @create 2021-09-09 11:30
 */


public class Flink04_Sink_Custom {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("CJhadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));

                return waterSensor;
            }
        });

        //TODO 自定义Sink写入Mysql
        map.addSink(new MySink());


        env.execute();
    }
    //自定义Sink将数据写入Mysql
    public static class MySink extends RichSinkFunction<WaterSensor> {
        private Connection connection;
        private PreparedStatement pstm;
        @Override
        public void open(Configuration parameters) throws Exception {

            System.out.println("创建连接");
            //获取连接
            connection = DriverManager.getConnection("jdbc:mysql://CJhadoop102:3306/test?useSSL=false", "root", "root");
            //语句预执行者
            pstm = connection.prepareStatement("insert into sensor values (?,?,?)");
        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {

            System.out.println("写入数据");
            pstm.setString(1, value.getId());
            pstm.setLong(2, value.getTs());
            pstm.setInt(3, value.getVc());

            //插入语句才真正执行
            pstm.execute();


        }

        @Override
        public void close() throws Exception {
            System.out.println("关闭连接");
            pstm.close();
            connection.close();
        }
    }
}
