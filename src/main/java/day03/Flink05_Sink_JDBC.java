package day03;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Driver;

/**
 * @author 刘帅
 * @create 2021-09-09 11:31
 */


public class Flink05_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("CJhadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] s = value.split(" ");

                return new WaterSensor(s[0], Long.parseLong(s[1]), Integer.parseInt(s[2]));
            }
        });

        //TODO 利用JDBC将数据写入Mysql
        map.addSink(JdbcSink.sink(
                "insert into sensor values (?,?,?)",
                (ps,t)->{
                    ps.setString(1,t.getId());
                    ps.setLong(2,t.getTs());
                    ps.setInt(3,t.getVc());
                },
                //与ES写入数据时一样，通过阈值控制什么时候写入数据，以下设置为来一条写一条
                new JdbcExecutionOptions.Builder().withBatchSize(1).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/test")
                        .withUsername("root")
                        .withPassword("000000")
                        //指定Driver全类名
                        .withDriverName(Driver.class.getName()).build()

        ));


        env.execute();
    }

}
