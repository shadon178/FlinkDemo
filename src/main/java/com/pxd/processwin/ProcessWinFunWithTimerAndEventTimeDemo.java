package com.pxd.processwin;

import com.pxd.source.ClickEvent;
import com.pxd.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 带定时器的ProcessFunction,必须先keyBy.
 */
public class ProcessWinFunWithTimerAndEventTimeDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(
                    (SerializableTimestampAssigner<ClickEvent>) (clickEvent, l) -> clickEvent.timestamp)
            )
            .keyBy(ClickEvent::getUserName)
            .process(new KeyedProcessFunction<String, ClickEvent, String>() {

                @Override
                public void processElement(
                    ClickEvent value,
                    KeyedProcessFunction<String, ClickEvent, String>.Context ctx,
                    Collector<String> out) {
                    long timestamp = ctx.timestamp();
                    out.collect(value.userName + "到达，时间戳: " + timestamp
                        + ", watermark: " + ctx.timerService().currentWatermark());

                    // 基于事件时间注册定时器
                    // 当源端的最后一个消息发来的时候，会触发所有未触发的定时器。
                    ctx.timerService().registerEventTimeTimer(timestamp + 1000);
                }

                @Override
                public void onTimer(
                    long timestamp,
                    KeyedProcessFunction<String, ClickEvent, String>.OnTimerContext ctx,
                    Collector<String> out) {
                    out.collect(ctx.getCurrentKey()
                        + "触发定时器，触发时间: " + timestamp
                        + ", watermark: " + ctx.timerService().currentWatermark());
                }
            })
            .print();
        env.execute();
    }

}
