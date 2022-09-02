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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.daemo
 * @date 2022-08-25-21:24
 * @Desc:
 */
public class _21_Window_Api3 {
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


        OutputTag<Tuple2<EventBean2, Integer>> lateTagOutputTag = new OutputTag<Tuple2<EventBean2, Integer>>("lateTag",TypeInformation.of(new TypeHint<Tuple2<EventBean2, Integer>>() {
        })) {
        };
        SingleOutputStreamOperator<Tuple2<EventBean2, Integer>> late = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(3)) //允许迟到的数据
                .sideOutputLateData(lateTagOutputTag) // 超过允许时限的数据
                .sum("f1");


        late.print();  //主流
        late.getSideOutput(lateTagOutputTag).print(); //迟到数据流

        env.execute();
    }
}
