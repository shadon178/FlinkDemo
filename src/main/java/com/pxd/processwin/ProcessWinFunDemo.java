package com.pxd.processwin;

import com.pxd.source.ClickEvent;
import com.pxd.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ProcessWinFunDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(
                    (SerializableTimestampAssigner<ClickEvent>) (clickEvent, l) -> clickEvent.timestamp)
            )
            .process(new ProcessFunction<ClickEvent, String>() {
                @Override
                public void processElement(
                    ClickEvent clickEvent,
                    ProcessFunction<ClickEvent, String>.Context context,
                    Collector<String> collector) {
                    if (clickEvent.userName.equals("pxd")) {
                        collector.collect("pxd click " + clickEvent.url);
                    }
                    System.out.println("timestamp: " + context.timestamp());
                    System.out.println("watermark: " + context.timerService().currentWatermark());
                    System.out.println("-----");
                }
            })
            .print();
        env.execute();
    }

}
