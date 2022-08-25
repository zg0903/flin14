package om.zg.daemo;

import lombok.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.daemo
 * @date 2022-08-21-21:06
 * @Desc:
 */
public class _20_Window_Api {

    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.setParallelism(1);

        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<EventBean2> beanStream = stream1.map(r -> {
            String[] split = r.split(",");
            return new EventBean2(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), Integer.valueOf(split[3]));
        }).returns(TypeInformation.of(EventBean2.class));


        SingleOutputStreamOperator<EventBean2> watermarkedBeanStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.
//                <EventBean2>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                <EventBean2>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean2>() {
                    @Override
                    public long extractTimestamp(EventBean2 element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }));

//        watermarkedBeanStream.print();

//     T1   每阁10s 统计最近30s中 每个用户的行为时间条数

        SingleOutputStreamOperator<Integer> t1 = watermarkedBeanStream.keyBy(r -> r.getGuid())
//                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
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

        SingleOutputStreamOperator<Double> t2 = watermarkedBeanStream.keyBy(r -> r.getGuid())
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
//        t2.print();

//        T3  每隔10s 最近30s的数据  每个用户行为事件中 行为时间最长的两条记录
        SingleOutputStreamOperator<EventBean2> t3 = watermarkedBeanStream.keyBy(r -> r.getGuid())
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .apply(new WindowFunction<EventBean2, EventBean2, Long, TimeWindow>() {
                    /**
                     *
                     * @param aLong The key for which this window is evaluated.
                     * @param window The window that is being evaluated.
                     * @param input The elements in the window being evaluated.
                     * @param out A collector for emitting elements.
                     * @throws Exception
                     */
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<EventBean2> input, Collector<EventBean2> out) throws Exception {

//                        low写法
                        ArrayList<EventBean2> tmpList = new ArrayList<>();
                        for (EventBean2 eventBean2 : input) {
                            tmpList.add(eventBean2);
                        }
                        Collections.sort(tmpList, new Comparator<EventBean2>() {
                            @Override
                            public int compare(EventBean2 o1, EventBean2 o2) {
                                return o2.getActTime() - o1.getActTime();
                            }
                        });

                        for (int i = 0; i < Math.min(2, tmpList.size()); i++) {
                            out.collect(tmpList.get(i));
                        }
//                        TreeMap<Integer, EventBean2> longEventBean2TreeMap = new TreeMap<>(new Comparator<Integer>() {
//                            @Override
//                            public int compare(Integer o1, Integer o2) {
//                                return o1 > o2 ? -1 : (o1 < o2) ? 1 : 0;
//                            }
//                        });
//                        for (EventBean2 event : input) {
//                            Integer actTime = event.getActTime();
//                            longEventBean2TreeMap.put(event.getActTime(), event);
//                        }
//                        Iterator<Integer> iterator = longEventBean2TreeMap.keySet().iterator();
//
//                        for (int i = 0; i < Math.min(2, longEventBean2TreeMap.size()); i++) {
//                            Integer next = iterator.next();
//                            out.collect(longEventBean2TreeMap.get(next));
//                        }

//                        for (EventBean2 event : input) {
//                            Integer actTime = event.getActTime();
//                            if (longEventBean2TreeMap.size() < 2) {
//                                longEventBean2TreeMap.put(actTime, event);
//                            } else if (longEventBean2TreeMap.size() == 2) {
////                                for (int i = 0; i < longEventBean2TreeMap.size(); i++) {
//                                for (int i = 0; i < 2; i++) {
//                                    Iterator<Integer> iterator = longEventBean2TreeMap.keySet().iterator();
//                                    Integer treeMapTop2ActTime = iterator.next();
//                                    if (actTime > treeMapTop2ActTime) {
//                                        longEventBean2TreeMap.remove(treeMapTop2ActTime);
//                                        longEventBean2TreeMap.put(actTime, event);
//                                    }
//
//                                }
//
//                            }
//
//                        }
//
//                        Iterator<Integer> iterator = longEventBean2TreeMap.keySet().iterator();
//                        for (int i = 0; i < Math.min(2, longEventBean2TreeMap.size()); i++) {
//                            Integer next = iterator.next();
//                            System.out.println(longEventBean2TreeMap.get(next));
//                            out.collect(longEventBean2TreeMap.get(next));
//                        }




                    }
                });

//        t1.print();
        t3.print();


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