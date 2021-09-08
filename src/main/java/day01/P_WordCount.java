package day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author 刘帅
 * @create 2021-09-03 10:18
 */


public class P_WordCount {
    public static void main(String[] args)  {
        // 1、创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2、从文件中读取数据，按行读取
        DataSet<String> lineDS = env.readTextFile("D:\\project\\maven_project\\Preview\\input\\word.txt");

        // 3、转换数据格式
        lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    /**
                     * out 采集器可以将数据收集发送至下游（因为匿名内部类吗，没有返回值）
                     */
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        //按照空格分词
                        String[] words = value.split(" ");

                        //遍历所有word，包成二元组输出
                        for (String word : words) {
                            out.collect(Tuple2.of(word,1));
                        }
                    }
                })
                 .groupBy(0)//按照第一个位置的word分组
                .sum(1);//将第二个位置上的数据求和

    }
}

