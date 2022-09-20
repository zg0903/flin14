package om.zg.daemo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.time.Time;
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
public class _23_State_OperatorState {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointStorage("file://d:chenkpoint/");

//        env.setRestartStrategy(RestartStrategies.noRestart()); // 一个task故障了 整个job就失败l

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000)); // 重启次数 间隔

        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9999);


        stream1.map(new Mymap())
                .print()
        ;


        env.execute();
    }
}


class Mymap implements MapFunction<String, String>, CheckpointedFunction {

    ListState<String> listState;

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

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

    }

    /**
     * 算子任务执行前 状态初始化
     *
     * @param context the context for initializing the operator
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //状态储存器
        OperatorStateStore operatorStateStore = context.getOperatorStateStore();
//        算子状态储存器 List结构储存数据
        ListStateDescriptor<String> stateDescriptor =
                new ListStateDescriptor<>("Strings", String.class);


//        unionListSatte 和普通 listState的区别:
//        unionListSatte 的快照存储数据 在系统重启后 list的数据重分配模式为 广播模式  在每个subtask上都拥有一份完整的数据
//        listState的快照存储数据 在系统重启后 加载状态数据时 系统自动进行状态数据的重平均分配


//        getListState 程序启动前重启持久化的状态 数据
        listState = operatorStateStore.getListState(stateDescriptor);
    }
}