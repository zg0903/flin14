package om.zg.daemo;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.daemo
 * @date 2022-08-25-21:24
 * @Desc: trigger
 */
public class _21_Window_Api4 {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.setParallelism(1);

        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<EventBean2, Integer>> beanStream = stream1.map(r -> {
            String[] split = r.split(",");
            EventBean2 bean = new EventBean2(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), Integer.valueOf(split[3]));
            return Tuple2.of(bean, 1);

        }).returns(new TypeHint<Tuple2<EventBean2, Integer>>() {
        });


        SingleOutputStreamOperator<Tuple2<EventBean2, Integer>> eventBean2SingleOutputStreamOperator =
                beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<EventBean2, Integer>>forBoundedOutOfOrderness(Duration.ofMinutes(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<EventBean2, Integer>>() {
                            @Override
                            public long extractTimestamp(Tuple2<EventBean2, Integer> element, long recordTimestamp) {
                                return element.f0.getTimestamp();
                            }
                        }));
        KeyedStream<Tuple2<EventBean2, Integer>, Long> keyedStream = eventBean2SingleOutputStreamOperator.keyBy(r -> r.f0.getGuid());


        OutputTag<Tuple2<EventBean2, Integer>> lateTagOutputTag = new OutputTag<Tuple2<EventBean2, Integer>>("lateTag", TypeInformation.of(new TypeHint<Tuple2<EventBean2, Integer>>() {
        })) {
        };
        SingleOutputStreamOperator<String> late = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .trigger(MyTrrigger.create())
//                .allowedLateness(Time.seconds(3)) //允许迟到的数据
//                .sideOutputLateData(lateTagOutputTag) // 超过允许时限的数据
//                .sum("f1");
//                .evictor(MyEvictor.create())
                .apply(new WindowFunction<Tuple2<EventBean2, Integer>, String, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<Tuple2<EventBean2, Integer>> input, Collector<String> out) throws Exception {
                        int count = 0;
                        for (Tuple2<EventBean2, Integer> ele : input) {
                            count++;
                        }
                        out.collect(window.getStart() + ":" + count + window.getEnd());
                    }
                });

        late.print();  //主流
//        late.getSideOutput(lateTagOutputTag).print(); //迟到数据流

        env.execute();
    }
}


class MyTrrigger extends Trigger<Tuple2<EventBean2, Integer>, TimeWindow> {

    @Override
    public TriggerResult onElement(Tuple2<EventBean2, Integer> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
//        正常触发
        if (window.maxTimestamp() < ctx.getCurrentWatermark()) {
            return TriggerResult.FIRE;
        } else {
//            注册定时器
            ctx.registerEventTimeTimer(window.maxTimestamp());
//            eventid 包含ex  触发
            if ("ex".equals(element.f0.getEventId())) return TriggerResult.FIRE;
        }

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return null;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

    }

    public static MyTrrigger create() {
        return new MyTrrigger();
    }


}


class MyEvictor extends TimeEvictor<TimeWindow>{

    public MyEvictor(long windowSize) {
        super(windowSize);
    }

    public MyEvictor(long windowSize, boolean doEvictAfter) {
        super(windowSize, doEvictAfter);
    }

//    public static Evictor<? super Tuple2<EventBean2, Integer>,? super TimeWindow> create() {
//        return new MyEvictor();
//    }

    @Override
    public void evictBefore(Iterable<TimestampedValue<Object>> elements, int size, TimeWindow window, EvictorContext ctx) {
        super.evictBefore(elements, size, window, ctx);
    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<Object>> elements, int size, TimeWindow window, EvictorContext ctx) {
        super.evictAfter(elements, size, window, ctx);
    }


    @Override
    public long getWindowSize() {
        return super.getWindowSize();
    }
}

