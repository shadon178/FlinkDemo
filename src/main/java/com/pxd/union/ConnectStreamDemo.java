package com.pxd.union;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 双流connect
 * 合并之后按顺序，轮流输出一个元素。
 */
public class ConnectStreamDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);
        DataStreamSource<Long> source2 = env.fromElements(30L, 20L, 10L, 50L);

        source1.connect(source2)
            .map(new CoMapFunction<Integer, Long, String>() {
                @Override
                public String map1(Integer value) {
                    return value.toString();
                }

                @Override
                public String map2(Long value) {
                    return value.toString();
                }
            })
            .print();

        env.execute();
    }

}
