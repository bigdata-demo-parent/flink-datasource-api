package com.flink.demo;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FromSequence {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 并行度为1
        env.setParallelism(1);

        // 通过generateSequence得到Long类型的DataSource
        DataStream<Long> dataStream = env.fromSequence(1, 10);

        // 做一次过滤，只保留偶数，然后打印
        dataStream.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
                return 0L == aLong % 2L;
            }
        }).print();

        env.execute("API DataSource demo : collection");
    }
}

