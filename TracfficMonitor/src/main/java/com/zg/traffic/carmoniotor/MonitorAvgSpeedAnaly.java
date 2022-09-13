package com.zg.traffic.carmoniotor;

import com.zg.traffic.vo.GlobalEntity;
import com.zg.traffic.vo.MonitorAvgSpeedInfo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name com.zg.traffic.carmoniotor
 * @date 2022-09-12-16:02
 * @Desc: 每隔1分钟 统计过去5分钟 每个区域每个道路每个卡扣的平均速度
 */
public class MonitorAvgSpeedAnaly {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        Kafka读取
        KafkaSource<String> kafasource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop104:9092,hadoop103:9092,")
                .setTopics("traffic-topic")
                .setGroupId("flink_group")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())

//                kafka消费者自动提交机制 吧最新的消费位移提交到kafka的consumer_offsets中
                .setProperty("auto.offset.commit", "true")
                .build();
        DataStreamSource<String> ds1 = env.fromSource(kafasource, WatermarkStrategy.noWatermarks(), "kafkaSource");

        //对车辆监控信息进行数据转换
        SingleOutputStreamOperator<GlobalEntity> carMonitrDs = ds1.map(new MapFunction<String, GlobalEntity>() {
            @Override
            public GlobalEntity map(String value) throws Exception {
                String[] split = value.split("\t");
                return new GlobalEntity(split[0], split[1], split[2], split[3], Long.parseLong(split[4]), split[5], Double.parseDouble(split[6]));
            }
        });

        SingleOutputStreamOperator<GlobalEntity> waterMarkDS = carMonitrDs.assignTimestampsAndWatermarks(WatermarkStrategy.<GlobalEntity>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
                new SerializableTimestampAssigner<GlobalEntity>() {
                    @Override
                    public long extractTimestamp(GlobalEntity element, long recordTimestamp) {
                        return element.getActionTime();
                    }
                }
        ));

        SingleOutputStreamOperator<MonitorAvgSpeedInfo> monitorAvgSpeedDS = waterMarkDS.keyBy(r -> r.getAreaId() + "_" + r.getRoadId() + "_" + r.getMonitorId())
//                        .timeWindow(Time.minutes(5),Time.minutes(1))
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .aggregate(new AggregateFunction<GlobalEntity, Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public Tuple2<Long, Double> createAccumulator() {
                        Tuple2<Long, Double> o = null;
                        return Tuple2.of(0L, 0.0);
                    }

                    @Override
                    public Tuple2<Long, Double> add(GlobalEntity value, Tuple2<Long, Double> accumulator) {
                        return Tuple2.of(accumulator.f0 + 1, accumulator.f1 + value.getSpeed());
                    }

                    @Override
                    public Tuple2<Long, Double> getResult(Tuple2<Long, Double> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Tuple2<Long, Double> merge(Tuple2<Long, Double> a, Tuple2<Long, Double> b) {
                        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                    }
                }, new WindowFunction<Tuple2<Long, Double>, MonitorAvgSpeedInfo, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Tuple2<Long, Double>> input, Collector<MonitorAvgSpeedInfo> out) throws Exception {
                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String start = df.format(new Date(window.getStart()));
                        String end = df.format(new Date(window.getEnd()));
                        double avgSpeed = 0.0;
                        Long carCountavgspeed = 0L;
                        for (Tuple2<Long, Double> ele : input) {
                            avgSpeed = ele.f1 / ele.f0;
                            carCountavgspeed = ele.f0;
                        }
                        out.collect(new MonitorAvgSpeedInfo(start, end, s, avgSpeed, carCountavgspeed));
                    }
                });

        monitorAvgSpeedDS.print();


        env.execute();
    }
}
