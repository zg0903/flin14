package om.zg.demo;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.demo
 * @date
 * @Desc:
 */


public class WaterDemo3 {
    public static void main(String[] args) throws Exception {


        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.setParallelism(1);

        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> mapped = stream.map(r -> {
            String[] split = r.split(",");
            return Tuple3.of(split[0], split[1], Long.valueOf(split[2]) * 1000);
        }).returns(new TypeHint<Tuple3<String, String, Long>>() {
        });


//        指定 水位线
        SingleOutputStreamOperator<Tuple3<String, String, Long>> tuple3SingleOutputStreamOperator = mapped.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>
                        forBoundedOutOfOrderness(Duration.ofSeconds(5)).
                withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                }));
        tuple3SingleOutputStreamOperator.print();

        tuple3SingleOutputStreamOperator.keyBy(r -> r.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(10))).
                process(new ProcessWindowFunction<Tuple3<String, String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Tuple3<String, String, Long>, String, String, TimeWindow>.Context context, Iterable<Tuple3<String, String, Long>> elements, Collector<String> out) throws Exception {
                        Long count = 0L;
                        for (Tuple3<String, String, Long> e : elements) {
                            count += 1;
                        }
                        String startTime = new Timestamp(context.window().getStart()).toString();
                        String endTime = new Timestamp(context.window().getEnd()).toString();
                        out.collect("窗口 " + startTime + " ~ " + endTime + " : " + count + " 条元素");

                    }
                })
                .print();


        env.execute();


    }
}
