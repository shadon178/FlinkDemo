package com.pxd.union;

import com.pxd.source.ClickEvent;
import com.pxd.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 使用侧输出流进行分流操作
 */
public class SplitStreamDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        OutputTag<String> outPutTag = new OutputTag<String>("aTag"){};

        SingleOutputStreamOperator<String> process = env.addSource(new ClickSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(
                    (SerializableTimestampAssigner<ClickEvent>) (clickEvent, l) -> clickEvent.timestamp)
            )
            .process(new ProcessFunction<ClickEvent, String>() {

                @Override
                public void processElement(
                    ClickEvent value,
                    ProcessFunction<ClickEvent, String>.Context ctx,
                    Collector<String> out) {
                    String userName = value.userName;
                    if ("pxd".equals(userName)) {
                        ctx.output(outPutTag, userName);
                    } else {
                        out.collect(userName);
                    }

                }

            });

        process.getSideOutput(outPutTag).print("aTag");

        process.print("else");

        env.execute();
    }

}
