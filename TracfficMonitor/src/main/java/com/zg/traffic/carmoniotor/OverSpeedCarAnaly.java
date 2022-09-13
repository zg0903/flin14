package com.zg.traffic.carmoniotor;

import com.zg.traffic.utils.JDBCSink;
import com.zg.traffic.vo.GlobalEntity;
import com.zg.traffic.vo.MonitorLimitSpeedInfo;
import com.zg.traffic.vo.OverSpeedCarInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.sql.*;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name com.zg.traffic.carmoniotor
 * @date 2022-09-12-9:38
 * @Desc:
 */
public class OverSpeedCarAnaly {

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


//        对limitSpeedDs 广播
//        key:areaId+roadId+monitorId  value:speedLimit
        MapStateDescriptor<String, Double> mapStateDescriptor = new MapStateDescriptor<String, Double>("limitSpeedMapState", String.class, Double.class) {
        };
        BroadcastStream<MonitorLimitSpeedInfo> limitSpeedInfoBroadcastStream = limitSpeedDs.broadcast(mapStateDescriptor);


        SingleOutputStreamOperator<OverSpeedCarInfo> overSpeedInfoDs = carMonitrDs.connect(limitSpeedInfoBroadcastStream).
                process(new BroadcastProcessFunction<GlobalEntity, MonitorLimitSpeedInfo, OverSpeedCarInfo>() {
                    @Override
                    public void processElement(GlobalEntity value, BroadcastProcessFunction<GlobalEntity, MonitorLimitSpeedInfo, OverSpeedCarInfo>.ReadOnlyContext ctx, Collector<OverSpeedCarInfo> out) throws Exception {
                        String areaId = value.getAreaId();
                        String roadId = value.getRoadId();
                        String monitorId = value.getMonitorId();
//                       本条数据的 区域 道路  卡扣
                        String key = areaId + "_" + roadId + "_" + monitorId;
                        //   获取广播状态
                        ReadOnlyBroadcastState<String, Double> readOnlyBroadcastState = ctx.getBroadcastState(mapStateDescriptor);

                        if (readOnlyBroadcastState.contains(key)) {
                            if (value.getSpeed() > readOnlyBroadcastState.get(key) * 1.2) {
                                out.collect(new OverSpeedCarInfo(value.getCar(), value.getMonitorId(), value.getRoadId(), value.getSpeed(), readOnlyBroadcastState.get(key), value.getActionTime()));
                            }
//                            如果没有从广播流中查到限速信息 则默认60限速
                            else if (value.getSpeed() > 60 * 1.2) {
                                out.collect(new OverSpeedCarInfo(value.getCar(), value.getMonitorId(), value.getRoadId(), value.getSpeed(), 60.0, value.getActionTime()));
                            }
                        }


                    }

                    @Override
                    public void processBroadcastElement(MonitorLimitSpeedInfo value, BroadcastProcessFunction<GlobalEntity, MonitorLimitSpeedInfo, OverSpeedCarInfo>.Context ctx, Collector<OverSpeedCarInfo> out) throws Exception {
                        BroadcastState<String, Double> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
//                        key:areaId+roadId+monitorId  value:speedLimit
                        broadcastState.put(value.getAreaId() + "_" + value.getRoadId() + "_" + value.getMonitorId(), value.getLimitSpeed());
                    }
                });
        overSpeedInfoDs.print();

        overSpeedInfoDs.addSink(new JDBCSink());


        env.execute();


    }

}
