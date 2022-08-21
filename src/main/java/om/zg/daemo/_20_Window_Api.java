package om.zg.daemo;

import lombok.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.daemo
 * @date 2022-08-21-21:06
 * @Desc:
 */
public class _20_Window_Api {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<EventBean2> beanStream = stream1.map(r -> {
            String[] split = r.split(",");
            return new EventBean2(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), Integer.valueOf(split[3]));
        }).returns(TypeInformation.of(EventBean2.class));


        SingleOutputStreamOperator<EventBean2> watermarkedBeanStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.
                <EventBean2>forBoundedOutOfOrderness(Duration.ofMillis(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean2>() {
                    @Override
                    public long extractTimestamp(EventBean2 element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }));
//     T1   每阁10s 统计最近30s中 每个用户的行为时间条数

        SingleOutputStreamOperator<Integer> userCount = watermarkedBeanStream.keyBy(r -> r.getGuid())
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .aggregate(new AggregateFunction<EventBean2, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(EventBean2 value, Integer accumulator) {
                        accumulator += 1;
                        return accumulator;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });
//        userCount.print();


        //     T2   每阁10s 统计最近30s中 每个用户平均行为时间

        watermarkedBeanStream.keyBy(r -> r.getGuid())
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .aggregate(new AggregateFunction<EventBean2, Tuple2<Integer, Integer>, Double>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return Tuple2.of(0, 0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(EventBean2 value, Tuple2<Integer, Integer> accumulator) {
//                        accumulator.setField(accumulator.f0 + 1, 0);
//                        accumulator.setField(accumulator.f1 + value.getActTime(), 1);
                        return Tuple2.of(accumulator.f0 + 1, accumulator.f1 + value.getActTime());
                    }

                    @Override
                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                        return accumulator.f1 / (double) accumulator.f0;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                    }
                });


        env.execute();


    }

    ;


}


@Data
@Getter
@Setter
@AllArgsConstructor
@ToString
class EventBean2 {
    private Long guid;
    private String eventId;
    private Long timestamp;
    private Integer ActTime;

}