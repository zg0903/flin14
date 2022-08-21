package om.zg.daemo;

import lombok.*;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.daemo
 * @date 2022-08-21-15:22
 * @Desc:
 */
public class _19_WaterMark_Api {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

//        WatermarkStrategy.noWatermarks()                  --禁用事件事件推进机制
//        WatermarkStrategy.forMonotonousTimestamps()       --紧跟最大事件事件
//        WatermarkStrategy.forBoundedOutOfOrderness()      --允许乱序
//        WatermarkStrategy.forGenerator()                  --自定义
        stream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(0))
//                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(0))
//                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
//                            @Override
//                            public long extractTimestamp(String element, long recordTimestamp) {
//                                return Long.parseLong(element.split(",")[2]);
//                            }
//                        })
                        .withTimestampAssigner((SerializableTimestampAssigner<String>) (element, recordTimestamp) -> Long.parseLong(element.split(",")[2]))
        );

        stream.map(r -> {
                    String[] split = r.split(",");
                    return new EventBean(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3]);
                }).returns(TypeInformation.of(EventBean.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<EventBean>forBoundedOutOfOrderness(Duration.ofMillis(10L)).
                        withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                            @Override
                            public long extractTimestamp(EventBean element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }));


        env.execute();


    }
}


@Data
@Getter
@Setter
@AllArgsConstructor
@ToString
class EventBean {
    private Long guid;
    private String eventId;
    private Long timestamp;
    private String pageId;

}