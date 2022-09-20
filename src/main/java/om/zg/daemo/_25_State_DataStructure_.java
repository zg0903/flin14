package om.zg.daemo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.daemo
 * @date 2022-09-05-21:28
 * @Desc:
 */
public class _25_State_DataStructure_ {
    public static void main(String[] args) throws Exception {
//        Configuration configuration = new Configuration();
//        configuration.setInteger("rest.port", 8081);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);


        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000)); // 重启次数 间隔

        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9999);

        stream1.keyBy(r -> "0")
                .map(new RichMapFunction<String, String>() {
                    ListState<String> listState;
                    MapState<String, String> mapState;
                    ValueState<String> valueState;

                    AggregatingState<Integer, Double> aggreState;

                    ReducingState<Integer> reduceState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        RuntimeContext runtimeContext = getRuntimeContext();
                        listState = runtimeContext.getListState(new ListStateDescriptor<String>("list", String.class));
                        mapState = runtimeContext.getMapState(new MapStateDescriptor<String, String>("map", String.class, String.class));
                        valueState = runtimeContext.getState(new ValueStateDescriptor<String>("vstate", String.class));

                        aggreState = runtimeContext.getAggregatingState(new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>(
                                "aggre", new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                            @Override
                            public Tuple2<Integer, Integer> createAccumulator() {
                                return Tuple2.of(0, 0);
                            }

                            @Override
                            public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                return Tuple2.of(accumulator.f0 + 1, accumulator.f1 + value);
                            }

                            @Override
                            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                return accumulator.f1 / (double) accumulator.f0;
                            }

                            @Override
                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                            }
                        }, TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
                        })
                        ));

                        reduceState = runtimeContext.getReducingState(new ReducingStateDescriptor<Integer>("reduce", new ReduceFunction<Integer>() {
                            @Override
                            public Integer reduce(Integer value1, Integer value2) throws Exception {
                                return value1 + value2;
                            }
                        }, Integer.class));


                    }

                    @Override
                    public String map(String value) throws Exception {
                        Iterable<String> strings = listState.get(); //获取liststate迭代
                        listState.add("a"); // 添加一个元素
                        listState.addAll(Arrays.asList("b", "c", "d")); // 添加多个元素
                        listState.update(Arrays.asList("1", "2")); //一次性将liststate中的数据替换为传入的元素

                        mapState.get("a");
                        mapState.put("a", "11");
                        mapState.contains("a");
                        Iterator<Map.Entry<String, String>> iterator = mapState.iterator();
                        Iterable<Map.Entry<String, String>> entries = mapState.entries();
                        boolean empty = mapState.isEmpty();


                        valueState.update("aaa");
                        valueState.value();

                        reduceState.add(1);

                        aggreState.add(10);
                        aggreState.add(20);
                        aggreState.get(); //15.0

                        return null;
                    }
                }).setParallelism(2)
                .print().setParallelism(2);


        env.execute();
    }
}
