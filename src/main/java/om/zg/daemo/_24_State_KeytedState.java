package om.zg.daemo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.daemo
 * @date 2022-09-04-17:19
 * @Desc:
 */
public class _24_State_KeytedState {
    public static void main(String[] args) throws Exception {
//        Configuration configuration = new Configuration();
//        configuration.setInteger("rest.port", 8081);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

//        env.getCheckpointConfig().setCheckpointStorage("file:///d:chenkpoint/");

//        env.setRestartStrategy(RestartStrategies.noRestart()); // 一个task故障了 整个job就失败l

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000)); // 重启次数 间隔

        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9999);

        stream1.keyBy(r -> r)
                .map(new RichMapFunction<String, String>() {
                    ListState<String> listState;
//                    StringBuilder stringBuilder = new StringBuilder();


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        RuntimeContext runtimeContext = getRuntimeContext();
                        listState = runtimeContext.getListState(new ListStateDescriptor<String>("list", String.class));

//                        List结构的状态储存器
//                        runtimeContext.getListState();
//                        单值 结构
//                        runtimeContext.getState();
//                        map结构
//                        runtimeContext.getMapState();

                    }

                    @Override
                    public String map(String value) throws Exception {

                        listState.add(value);
                        Iterable<String> strings = listState.get();
                        StringBuilder stringBuilder = new StringBuilder();
                        for (String string : strings) {
                            stringBuilder.append(string);
                        }

                        return stringBuilder.toString();
                    }
                }).setParallelism(2)
                .print().setParallelism(2);


        env.execute();
    }
}

