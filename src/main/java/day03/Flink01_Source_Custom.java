package day03;

import bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author 刘帅
 * @create 2021-09-08 8:57
 */


public class Flink01_Source_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<WaterSensor> streamSource = env.addSource(new Mysource());

        streamSource.print();

        env.execute();

    }

    public static class Mysource implements SourceFunction<WaterSensor> {

        private Random random = new Random();
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<WaterSensor> sourceContext) throws Exception {
            while (isRunning) {
            sourceContext.collect(new WaterSensor("s_" + random.nextInt(1000),System.currentTimeMillis(),random.nextInt(100)));
            }
        }

        @Override
        public void cancel() {
        isRunning = false;
        }
    }
}
