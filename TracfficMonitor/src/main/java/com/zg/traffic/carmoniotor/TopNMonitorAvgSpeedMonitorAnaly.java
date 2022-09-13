package com.zg.traffic.carmoniotor;

import com.zg.traffic.utils.TimeUtils;
import com.zg.traffic.vo.GlobalEntity;
import com.zg.traffic.vo.MonitorSpeedCount;
import com.zg.traffic.vo.Top5MonitorInfo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;
import java.util.*;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name com.zg.traffic.vo
 * @date 2022-09-12-18:09
 * @Desc: 每隔一分钟统计最近5分钟最通畅的卡扣信息
 */
public class TopNMonitorAvgSpeedMonitorAnaly {
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


        SingleOutputStreamOperator<Top5MonitorInfo> rs = waterMarkDS.windowAll(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .process(new ProcessAllWindowFunction<GlobalEntity, Top5MonitorInfo, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<GlobalEntity, Top5MonitorInfo, TimeWindow>.Context context, Iterable<GlobalEntity> elements, Collector<Top5MonitorInfo> out) throws Exception {

                        HashMap<String, MonitorSpeedCount> map = new HashMap<>();
                        Iterator<GlobalEntity> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            GlobalEntity currentInfo = iterator.next();
                            String areaId = currentInfo.getAreaId();
                            String roadId = currentInfo.getRoadId();
                            String monitorId = currentInfo.getMonitorId();
                            String currentkey = areaId + "_" + roadId + "_" + monitorId;
                            if (currentkey.contains(currentkey)) {
//                                        判断当前此条车辆速度位于哪个速度段  对应的value MonitorSpeedCount对象对应的速度加1
                                MonitorSpeedCount monitorSpeedCount = map.get(currentInfo);
                                if (currentInfo.getSpeed() >= 120) {
                                    monitorSpeedCount.setXhighSpeedCarCountPlusOne();
                                } else if (currentInfo.getSpeed() >= 90) {
                                    monitorSpeedCount.setXmiddleSpeedCarCountPlusOne();
                                } else if (currentInfo.getSpeed() >= 60) {
                                    monitorSpeedCount.setXmiddleSpeedCarCountPlusOne();
                                } else {
                                    monitorSpeedCount.setXlowSpeedCarCountPlusOne();
                                }
                                map.put(currentkey, monitorSpeedCount);
                            } else {
                                map.put(currentkey, new MonitorSpeedCount(0L, 0L, 0L, 0L));
                            }
                        }
                        LinkedHashMap<String, MonitorSpeedCount> sortedMap = map.entrySet().stream().sorted(comparingByValue())
                                .collect(
                                        toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                                                LinkedHashMap::new)
                                );
                        Iterator<Map.Entry<String, MonitorSpeedCount>> iterator1 = sortedMap.entrySet().iterator();
                        for (int i = 0; i < 5; i++) {
                            Map.Entry<String, MonitorSpeedCount> next = iterator1.next();
                            String key = next.getKey();
                            MonitorSpeedCount value = next.getValue();
                            String startTime = TimeUtils.timeStampToDataStr(context.window().getStart());
                            String endTime = TimeUtils.timeStampToDataStr(context.window().getEnd());
                            out.collect(new Top5MonitorInfo(startTime, endTime, key, value.getXhighSpeedCarCount(),
                                    value.getXmiddleSpeedCarCount(), value.getXnormalSpeedCarCount(), value.getXlowSpeedCarCount()));

                        }


                    }
                });

        rs.print();


        env.execute();
    }
}
