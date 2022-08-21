package om.zg.daemo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.daemo
 * @date 2022-08-20-16:23
 * @Desc:
 */
public class _11_sideoutputstream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9090);


        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split("\t");
                for (String s : split) {
                    out.collect(Tuple2.of(s, 1));
                }

            }

        });

        OutputTag<outputBean> a = new OutputTag<outputBean>("A") {
        };
        OutputTag<outputBean> b = new OutputTag<outputBean>("B") {
        };


//        processFunction
        SingleOutputStreamOperator<outputBean> sideoutput = flatMap.process(
                new ProcessFunction<Tuple2<String, Integer>, outputBean>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> value, ProcessFunction<Tuple2<String, Integer>, outputBean>.Context ctx, Collector<outputBean> out) throws Exception {

                        if (value.f0.toLowerCase().contains("a")) {
                            ctx.output(a, new outputBean(value.f0, value.f1));
                        } else if (value.f0.toLowerCase().contains("b")) {
                            ctx.output(b, new outputBean(value.f0, value.f1));
                        } else {
                            out.collect(new outputBean(value.f0, value.f1));
                        }


                    }
                }

        );
        DataStream<outputBean> sideOutput_a = sideoutput.getSideOutput(a);
        DataStream<outputBean> sideOutput_b = sideoutput.getSideOutput(b);
        sideoutput.print();


//        connect
        ConnectedStreams<outputBean, outputBean> connect = sideOutput_a.connect(sideOutput_b);
        connect.map(new CoMapFunction<outputBean, outputBean, outputBean>() {
            private String co = "cc";

            @Override
            public outputBean map1(outputBean value) throws Exception {
                return new outputBean(value.getChacter() + "coonected_a" + co, value.getNum() + 18);
            }

            @Override
            public outputBean map2(outputBean value) throws Exception {
                return new outputBean(value.getChacter() + "coonected_b" + co, value.getNum() + 20);
            }
        }).print();

//        union
        DataStream<outputBean> union = sideOutput_a.union(sideOutput_b);
//        union.print();


        DataStream<outputBean> apply = union.coGroup(sideOutput_b).where(new KeySelector<outputBean, String>() {
                    @Override
                    public String getKey(outputBean value) throws Exception {
                        return value.getChacter();
                    }
                }).equalTo(new KeySelector<outputBean, String>() {
                    @Override
                    public String getKey(outputBean value) throws Exception {
                        return value.getChacter();
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<outputBean, outputBean, outputBean>() {
                    @Override
                    public void coGroup(Iterable<outputBean> first, Iterable<outputBean> second, Collector<outputBean> out) throws Exception {
                        for (outputBean f : first) {
                            for (outputBean s : second) {
                                out.collect(new outputBean(f.getChacter() + s.getChacter() + "_cogroup", f.getNum() + s.getNum()));
                            }
                        }
                    }
                });

//        apply.print();

//        join ->cogroup封装
        DataStream<outputBean> joined = union.join(sideOutput_b).where(r -> r.getChacter()).equalTo(s -> s.getChacter()).window(TumblingProcessingTimeWindows.of(Time.seconds(10))).
                apply(new JoinFunction<outputBean, outputBean, outputBean>() {
                    @Override
                    public outputBean join(outputBean first, outputBean second) throws Exception {
                        return new outputBean(first.getChacter() + second.getChacter() + "_join_", first.getNum() + second.getNum())
                                ;
                    }
                });
        joined.print();

        env.execute();
    }
}

@Data
@Getter
@Setter
@AllArgsConstructor
class outputBean {
    private String chacter;
    private Integer num;
}
