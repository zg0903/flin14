package om.zg.daemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.daemo
 * @date 2022-08-20-10:19
 * @Desc:
 */
public class _01_wordcont_java {
    public static void main(String[] args) throws Exception {

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);


        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

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
                .keyBy(r -> r.f0).sum(1)


                .print();

        env.execute();

    }
}
