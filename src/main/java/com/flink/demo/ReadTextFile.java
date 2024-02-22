package com.flink.demo;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ReadTextFile {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);

        // 用txt文件作为数据源
        String filePath = ReadTextFile.class.getClassLoader().getResource("test.txt").getPath();
        DataStream<String> textDataStream = env.readTextFile(filePath, "UTF-8");

        // 统计单词数量并打印出来
        textDataStream.flatMap(new Splitter()).keyBy(0).sum(1).print();

        env.execute("API DataSource demo : readTextFile");
    }


    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}

