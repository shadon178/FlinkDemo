package com.pxd.source;

import org.apache.flink.shaded.curator4.com.google.common.collect.ImmutableList;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.List;

public class ClickSource extends RichSourceFunction<ClickEvent> {

    private final List<String> userNames = ImmutableList.of("pxd", "b", "c", "d", "e");

    private final List<String> urls = ImmutableList.of("a", "b", "c", "d", "e");

    @Override
    public void run(SourceContext<ClickEvent> sourceContext) throws Exception {
        while (true) {
            ClickEvent event = new ClickEvent();
            event.userName = userNames.get((int) (Math.random() * userNames.size()));
            event.url = urls.get((int) (Math.random() * urls.size()));
            event.timestamp = System.currentTimeMillis();
            sourceContext.collect(event);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }

}
