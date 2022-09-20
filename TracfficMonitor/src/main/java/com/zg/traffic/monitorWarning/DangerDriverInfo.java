package com.zg.traffic.monitorWarning;

import com.zg.traffic.utils.TimeUtils;
import com.zg.traffic.vo.GlobalEntity;
import com.zg.traffic.vo.MonitorLimitSpeedInfo;
import com.zg.traffic.vo.NewMonitorCarInfo;
import com.zg.traffic.vo.ViolationCarInfo;
import javassist.expr.NewArray;
import jdk.internal.org.objectweb.asm.tree.analysis.Value;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.sql.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name com.zg.traffic.monitorWarning
 * @date 2022-09-20-21:26
 * @Desc: 一辆车5分钟内连续超速3个卡扣 危险驾驶
 */
public class DangerDriverInfo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

//        DataStreamSource<String> ds1 = env.socketTextStream("localhost", 9999);


        //对车辆监控信息进行数据转换
        SingleOutputStreamOperator<GlobalEntity> carMonitrDs = ds1.map(new MapFunction<String, GlobalEntity>() {
            @Override
            public GlobalEntity map(String value) throws Exception {
                String[] split = value.split("\t");
                return new GlobalEntity(split[0], split[1], split[2], split[3], Long.parseLong(split[4]), split[5], Double.parseDouble(split[6]));
            }
        });

        carMonitrDs.assignTimestampsAndWatermarks(WatermarkStrategy.<GlobalEntity>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<GlobalEntity>() {
                    @Override
                    public long extractTimestamp(GlobalEntity element, long recordTimestamp) {
                        return element.getActionTime();
                    }
                }));

        SingleOutputStreamOperator<NewMonitorCarInfo> newMonitorCarInfoSingleOutputStreamOperator = carMonitrDs.map(new RichMapFunction<GlobalEntity, NewMonitorCarInfo>() {

            Connection coon = null;
            PreparedStatement pst = null;
            boolean flag = false;

            @Override
            public void open(Configuration parameters) throws Exception {
                coon = DriverManager
                        .getConnection("jdbc:mysql://localhost:3306/traffic_monitor?characterEncoding=UTF-8&serverTimezone=GMT%2B8",
                                "root", "mima0903!");
                pst = coon.prepareStatement("select area_id,road_id,monitor_id,speed_limit from t_monitor_speedlimit_info " +
                        "where area_id = ? and road_id =? and monitor_id = ? ");
            }

            @Override
            public NewMonitorCarInfo map(GlobalEntity value) throws Exception {
                String areaId = value.getAreaId();
                String roadId = value.getRoadId();
                String monitorId = value.getMonitorId();
                String cameraId = value.getCameraId();
                Long actionTime = value.getActionTime();

                pst.setString(1, areaId);
                pst.setString(2, roadId);
                pst.setString(3, monitorId);
                ResultSet resultSet = pst.executeQuery();
                Double limitSpeed = 60.0;
                while (resultSet.next()) {
                    resultSet.getDouble("speed_limit");
                }
                return new NewMonitorCarInfo(areaId, roadId, monitorId, cameraId, actionTime, value.getCar(),
                        value.getSpeed(), limitSpeed);
            }


            @Override
            public void close() throws Exception {
                flag = true;
                try {
                    pst.close();
                    coon.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        });


        Pattern<NewMonitorCarInfo, NewMonitorCarInfo> pattern = Pattern.<NewMonitorCarInfo>begin("first")
                .where(new SimpleCondition<NewMonitorCarInfo>() {
                    @Override
                    public boolean filter(NewMonitorCarInfo value) throws Exception {
                        return value.getSpeed() > value.getSpeedLimit() * 1.2;
                    }
                })
                .followedBy("second")
                .where(new SimpleCondition<NewMonitorCarInfo>() {
                    @Override
                    public boolean filter(NewMonitorCarInfo value) throws Exception {
                        return value.getSpeed() > value.getSpeedLimit() * 1.2;
                    }
                })
                .followedBy("third")
                .where(new SimpleCondition<NewMonitorCarInfo>() {
                    @Override
                    public boolean filter(NewMonitorCarInfo value) throws Exception {
                        return value.getSpeed() > value.getSpeedLimit() * 1.2;
                    }
                })
                .within(Time.seconds(10));

        PatternStream<NewMonitorCarInfo> ps = CEP.pattern(newMonitorCarInfoSingleOutputStreamOperator.keyBy(r -> r.getCar())
                , pattern);

        SingleOutputStreamOperator<ViolationCarInfo> rs = ps.select(new PatternSelectFunction<NewMonitorCarInfo, ViolationCarInfo>() {
            @Override
            public ViolationCarInfo select(Map<String, List<NewMonitorCarInfo>> pattern) throws Exception {

                NewMonitorCarInfo firstInfo = pattern.get("first").iterator().next();
                NewMonitorCarInfo secondInfo = pattern.get("second").iterator().next();
                NewMonitorCarInfo thirdInfo = pattern.get("third").iterator().next();

                String details = firstInfo.getCar() + " first:" + firstInfo.getMonitorId() + firstInfo.getActionTime()
                        + " second:" + secondInfo.getMonitorId() + firstInfo.getActionTime()
                        + " third:" + thirdInfo.getMonitorId() + firstInfo.getActionTime();

                return new ViolationCarInfo(firstInfo.getCar(), "涉嫌危险驾驶",
                        TimeUtils.timeStampToDataStr(System.currentTimeMillis()), details
                );
            }
        });


//      从MySQL 读取-道路-卡扣-车辆限速信息  每隔半小时读取一次
        DataStreamSource<MonitorLimitSpeedInfo> limitSpeedDs = env.addSource(new RichSourceFunction<MonitorLimitSpeedInfo>() {
            Connection coon = null;
            PreparedStatement pst = null;
            boolean flag = false;

            @Override
            public void open(Configuration parameters) throws Exception {
                coon = DriverManager
                        .getConnection("jdbc:mysql://localhost:3306/traffic_monitor?characterEncoding=UTF-8&serverTimezone=GMT%2B8",
                                "root", "mima0903!");
                pst = coon.prepareStatement("select area_id,road_id,monitor_id,speed_limit from t_monitor_speedlimit_info");

            }

            @Override
            public void run(SourceContext<MonitorLimitSpeedInfo> ctx) throws Exception {
                while (!flag) {
                    ResultSet resultSet = pst.executeQuery();
                    while (resultSet.next()) {
                        String areaId = resultSet.getString("area_id");
                        String roadId = resultSet.getString("road_id");
                        String monitorId = resultSet.getString("monitor_id");
                        Double speedLimit = resultSet.getDouble("speed_limit");
                        ctx.collect(new MonitorLimitSpeedInfo(areaId, roadId, monitorId, speedLimit));

                    }
                    Thread.sleep(30 * 1000 * 60);
                }


            }

            @Override
            public void cancel() {
                flag = true;
                try {
                    pst.close();
                    coon.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

            }
        });

        env.execute();

    }
}
