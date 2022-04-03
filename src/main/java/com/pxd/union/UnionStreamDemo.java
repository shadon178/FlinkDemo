package com.pxd.union;

import com.pxd.source.ClickEvent;
import com.pxd.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 双流合并
 * 注意：双流合并时，watermark取最小值。
 */
public class UnionStreamDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<ClickEvent> stream1 = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(
                    (SerializableTimestampAssigner<ClickEvent>) (clickEvent, l) -> clickEvent.timestamp)
            );

        SingleOutputStreamOperator<ClickEvent> stream2 = env.addSource(new ClickSource())
            .map(clickEvent -> {
                clickEvent.setUserName("pxd2");
                return clickEvent;
            })
            .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(
                    (SerializableTimestampAssigner<ClickEvent>) (clickEvent, l) -> clickEvent.timestamp)
            );

        stream2.print("stream2");

        stream1.union(stream2)
            .process(new ProcessFunction<ClickEvent, String>() {
            @Override
            public void processElement(ClickEvent value,
                                       ProcessFunction<ClickEvent, String>.Context ctx,
                                       Collector<String> out) {
                out.collect("watermark: " + new Timestamp(ctx.timerService().currentWatermark()));
            }
        }).print();

        env.execute();
    }

}
