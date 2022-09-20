package om.zg.daemo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.daemo
 * @date 2022-09-05-21:28
 * @Desc:
 */
public class _26_State_TTL_ {
    public static void main(String[] args) throws Exception {
//        Configuration configuration = new Configuration();
//        configuration.setInteger("rest.port", 8081);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend();
        HashMapStateBackend hashMapStateBackend = new HashMapStateBackend();
//        FsStateBackend fsStateBackend = new FsStateBackend();
//        new MemoryStateBackend();
        env.setStateBackend(hashMapStateBackend); //  使用hashmap状态后端-
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);


        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000)); // 重启次数 间隔

        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9999);

        stream1.keyBy(r -> "0")
                .map(new RichMapFunction<String, String>() {
                    ListState<String> listState;
                    ValueState<String> valueState;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        RuntimeContext runtimeContext = getRuntimeContext();

                        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(5))
                                .setTtl(Time.seconds(5)) //配置存活时间
                                .updateTtlOnCreateAndWrite() //插入更新刷新ttl计时
//                                .updateTtlOnReadAndWrite()// 读写刷新计时
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //设置状态的可见性(过期 真正清除)
//                                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                                .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime) // ttl计时时间语义
                                .useProcessingTime()
//                                清理策略
                                .cleanupIncrementally(1000, true) // 增量清理 每当一条数据被访问 就会检查ttl 是就删除
                                .cleanupFullSnapshot() //全量快照清理策略 在checkpoints时候保存到快照文件中的只包含未过期的状态数据 但它不会清理本地的状态数据
                                .cleanupInRocksdbCompactFilter(1000) //增量清理 支队rockstatebackend有效
//                                .disableCleanupInBackground()
                                .build();

                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("vstate", String.class);
                        valueStateDescriptor.enableTimeToLive(stateTtlConfig);
//                        valueState = runtimeContext.getState(valueStateDescriptor);

                        ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("list", String.class);
                        listStateDescriptor.enableTimeToLive(stateTtlConfig);
                        listState = runtimeContext.getListState(listStateDescriptor);
                    }

                    @Override
                    public String map(String value) throws Exception {
                        listState.add(value);
                        Iterable<String> strings = listState.get(); //获取liststate迭代
                        StringBuilder stringBuilder = new StringBuilder();
                        for (String string : strings) {
                            stringBuilder.append(string);
                        }
                        return stringBuilder.toString();
                    }
                })
                .print();
        env.execute();
    }
}
