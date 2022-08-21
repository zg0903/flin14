package om.zg.exercise;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.exercise
 * @date 2022-08-21-11:33
 * @Desc:
 */


/*
 *  stream 1    id,event,cnt
 * stream2   id gender,city
 * 需求
 *
 * */


public class Exericse_1 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9998);

        SingleOutputStreamOperator<event> s1_mapped = stream1.map(r -> {
            String[] split = r.split(",");
            event event = new event(Integer.parseInt(split[0]), split[1], Integer.parseInt(split[2]));
            return event;
        }).returns(TypeInformation.of(event.class));


        SingleOutputStreamOperator<person> s2_mapped = stream2.map(r -> {
            String[] split = r.split(",");
            return new person(Integer.parseInt(split[0]), split[1], split[2]);
        }).returns(TypeInformation.of(person.class));


//        T1   将流1的书展开  随机三条
        SingleOutputStreamOperator<event> s1_processed = s1_mapped.process(new ProcessFunction<event, event>() {
            @Override
            public void processElement(event value, ProcessFunction<event, event>.Context ctx, Collector<event> out) throws Exception {
                for (int i = 0; i < value.getCnt(); i++) {
                    out.collect(new event(value.getId(), value.getEnvntId(), (int) Math.random() * value.getCnt()));
                }

            }
        });

//      T2 流1数据关联上流二数据   加上 性别 城市
        DataStream<event_person> s1_s2_joined = s1_mapped.join(s2_mapped).where(l -> l.getId()).equalTo(r -> r.getId()).window(TumblingProcessingTimeWindows.of(Time.seconds(180)))
                .apply(new JoinFunction<event, person, event_person>() {
                    @Override
                    public event_person join(event first, person second) throws Exception {

                        return new event_person(first.getId(), first.getEnvntId(), first.getCnt(), second.getGender(), second.getCity());
                    }
                });

//        T2 把关联上cnt能被7 整除的放入一个侧输出流 其他放主输出流
        OutputTag<event_person> diviedBySeven = new OutputTag<event_person>("diviedBySeven") {
        };
        SingleOutputStreamOperator<event_person> outputed = s1_s2_joined.process(new ProcessFunction<event_person, event_person>() {
            @Override
            public void processElement(event_person element, ProcessFunction<event_person, event_person>.Context ctx, Collector<event_person> out) throws Exception {
                if (element.getCnt() % 7 == 0) {
                    ctx.output(diviedBySeven, element);
                } else {
                    out.collect(element);
                }
            }
        });

        DataStream<event_person> diviedBySevensideOutput = outputed.getSideOutput(diviedBySeven);


//        T4 对主流按照性别分组,输出最大随机数所在的那一条数据
        SingleOutputStreamOperator<event_person> keyedMax = outputed.keyBy(r -> r.getGender()).reduce(new ReduceFunction<event_person>() {
            @Override
            public event_person reduce(event_person value1, event_person value2) throws Exception {
                if (value2.getCnt() >= value1.getCnt()) {
                    return value2;
                } else {
                    return value1;
                }
            }
        });


        s1_mapped.print("s1_mapped:");
        s2_mapped.print("s2_mapped:");
        s1_s2_joined.print("s1_s2_joined:");
        outputed.print("outputed: ");
        diviedBySevensideOutput.print("diviedBySevensideOutput:");
        keyedMax.print("keyedMax:");
        env.execute();


    }


}

@AllArgsConstructor
@Getter
@Setter
@Data
class event {
    private Integer id;
    private String envntId;
    private int cnt;
}


@AllArgsConstructor
@Getter
@Setter
@Data
class person {
    private Integer id;
    private String Gender;
    private String city;

}

@AllArgsConstructor
@Getter
@Setter
@Data
class event_person {
    private Integer id;
    private String envntId;
    private int cnt;
    private String Gender;
    private String city;
}




