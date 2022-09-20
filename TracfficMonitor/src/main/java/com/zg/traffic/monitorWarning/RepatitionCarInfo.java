package com.zg.traffic.monitorWarning;

import com.zg.traffic.utils.TimeUtils;
import com.zg.traffic.vo.GlobalEntity;
import com.zg.traffic.vo.ViolationCarInfo;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name com.zg.traffic.monitorWarning
 * @date 2022-09-14-21:46
 * @Desc: 套牌车辆监控
 */


public class RepatitionCarInfo {
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


//        状态检测 套牌
        carMonitrDs.keyBy(r -> r.getCar()).process(new KeyedProcessFunction<String, GlobalEntity, ViolationCarInfo>() {

            ValueState<Long> valueState;

            //给每辆车设置状态 储存的是最近这辆车结果卡扣的时间
            @Override
            public void open(Configuration parameters) throws Exception {
                RuntimeContext runtimeContext = getRuntimeContext();
                valueState = runtimeContext.getState(new ValueStateDescriptor<Long>("carTimeState", Long.class));
            }

            @Override
            public void processElement(GlobalEntity value, KeyedProcessFunction<String, GlobalEntity, ViolationCarInfo>.Context ctx, Collector<ViolationCarInfo> out) throws Exception {
//            判断对应车辆是否有对应状态值
                Long preLastActionTimend = valueState.value();
                Long actionTime = value.getActionTime();
                String car = value.getCar();
                if (preLastActionTimend == 0) {
//                    首次更新状态
                    valueState.update(actionTime);
                } else {
//                    没有 获取时间 与当前时间做差值是否超过10S
                    if (Math.abs(actionTime - preLastActionTimend) < 10 * 1000) {
//                        涉嫌套牌
                        String detail = "车辆" + car + "通过上一次卡扣时间" + TimeUtils.timeStampToDataStr(preLastActionTimend) +
                                "本次通过时间" + TimeUtils.timeStampToDataStr(actionTime);
                        out.collect(new ViolationCarInfo(car, "涉嫌套牌", TimeUtils.timeStampToDataStr(System.currentTimeMillis())
                                , detail));
                    }
//                    更新状态
                    valueState.update(Math.max(preLastActionTimend, actionTime));
                }

            }
        });


        env.execute();


    }
}
