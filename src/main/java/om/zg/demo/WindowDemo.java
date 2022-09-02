package om.zg.demo;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Iterator;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.demo
 * @date
 * @Desc:
 */


public class WindowDemo {
    public static void main(String[] args) throws Exception {


        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.setParallelism(1);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> mapped = stream.map(r -> {
            String[] split = r.split(",");
            return Tuple3.of(split[0], split[1], Integer.valueOf(split[2]));
        }).returns(new TypeHint<Tuple3<String, String, Integer>>() {
        });


        mapped.keyBy(r -> r.f1).countWindow(3)
                .apply(new WindowFunction<Tuple3<String, String, Integer>, Integer, String, GlobalWindow>() {
                    @Override
                    public void apply(String s, GlobalWindow window, Iterable<Tuple3<String, String, Integer>> input, Collector<Integer> out) throws Exception {
                        Integer sum = 0;
                        Iterator<Tuple3<String, String, Integer>> iterator = input.iterator();
                        while (iterator.hasNext()) {
                            sum += iterator.next().f2;
                        }
                        out.collect(sum);
                    }
                })
//                .print()
        ;


        mapped.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10))).process(new ProcessAllWindowFunction<Tuple3<String, String, Integer>, String, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<Tuple3<String, String, Integer>, String, TimeWindow>.Context context, Iterable<Tuple3<String, String, Integer>> elements, Collector<String> out) throws Exception {
                Integer count = 0;
                for (Tuple3<String, String, Integer> e : elements) {
                    count += 1;
                }
                String windowStart = new Timestamp(context.window().getStart()).toString();
                String windowEnd = new Timestamp(context.window().getEnd()).toString();
                out.collect("窗口 " + windowStart + " ~ " + windowEnd + " : " + count + " 条元素");
            }
        }).print();


        env.execute();


    }
}
