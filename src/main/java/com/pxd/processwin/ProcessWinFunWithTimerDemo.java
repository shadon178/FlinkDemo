package com.pxd.processwin;

import com.pxd.source.ClickEvent;
import com.pxd.source.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 带定时器的ProcessFunction,必须先keyBy.
 */
public class ProcessWinFunWithTimerDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
            .keyBy(ClickEvent::getUserName)
            .process(new KeyedProcessFunction<String, ClickEvent, String>() {

                @Override
                public void processElement(
                    ClickEvent value,
                    KeyedProcessFunction<String, ClickEvent, String>.Context ctx,
                    Collector<String> out) {
                    long processingTime = ctx.timerService()
                        .currentProcessingTime();
                    out.collect(value.userName + "到达，Processing time: " + processingTime);
                    ctx.timerService().registerProcessingTimeTimer(processingTime + 1000);
                }

                @Override
                public void onTimer(
                    long timestamp,
                    KeyedProcessFunction<String, ClickEvent, String>.OnTimerContext ctx,
                    Collector<String> out) {
                    out.collect(ctx.getCurrentKey()
                        + "触发定时器，Processing time: "
                        + timestamp);
                }
            })
            .print();
        env.execute();
    }

}
