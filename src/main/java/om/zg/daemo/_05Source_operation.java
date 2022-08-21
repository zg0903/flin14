package om.zg.daemo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.ArrayList;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.daemo
 * @date 2022-08-20-13:04
 * @Desc:
 */
public class _05Source_operation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//        env.fromCollection();

//        kafkasource

        KafkaSource<String> kafasource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setTopics("flink14")
                .setGroupId("flink_group")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())

//                kafka消费者自动提交机制 吧最新的消费位移提交到kafka的consumer_offsets中
                .setProperty("auto.offset.commit", "true")
                .build();


        DataStreamSource<String> kafkaSource = env.fromSource(kafasource, WatermarkStrategy.noWatermarks(), "kafkaSource");

        kafkaSource.print();


        env.execute();


    }


}
