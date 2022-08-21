package om.zg.daemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.daemo
 * @date 2022-08-20-15:08
 * @Desc:
 */
public class _07_transform {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9090);


        streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] split = value.split("\t");
                        for (String s : split) {
                            out.collect(Tuple2.of(s, 1));
                        }

                    }

                })
                .keyBy(r -> r.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        value2.setField(value2.f1 + value1.f1, 1);
                        return value2;

                    }
                }).print();
        env.execute();

    }
}
