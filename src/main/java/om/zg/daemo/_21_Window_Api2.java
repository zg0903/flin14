package om.zg.daemo;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.daemo
 * @date 2022-08-25-21:24
 * @Desc:
 */
public class _21_Window_Api2 {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.setParallelism(1);

        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<EventBean2> beanStream = stream1.map(r -> {
            String[] split = r.split(",");
            return new EventBean2(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), Integer.valueOf(split[3]));
        }).returns(TypeInformation.of(EventBean2.class));


//       全局计数滚动窗口
        beanStream.countWindowAll(10) // 10条数据一个窗口
                .apply(new AllWindowFunction<EventBean2, Object, GlobalWindow>() {
                    @Override
                    public void apply(GlobalWindow window, Iterable<EventBean2> values, Collector<Object> out) throws Exception {


                    }
                });


//        全局计数滑动窗口
        beanStream.countWindowAll(10, 2);


//        全局事件时间滚动窗口
        beanStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(30))) //
                .apply(new AllWindowFunction<EventBean2, Object, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<EventBean2> values, Collector<Object> out) throws Exception {

                    }
                });


//        全局 事件时间滑动窗口
        beanStream.windowAll(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)));

//        全局 事件时间会话窗口
        beanStream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(30))); // 前后超过30s 划分窗口

//       全局 处理时间滑动窗口
        beanStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)));

//        全局 处理时间滑动窗口
        beanStream.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)));


//        全局 处理时间会话敞口
        beanStream.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));


        KeyedStream<EventBean2, Long> keyedStream = beanStream.keyBy(r -> r.getGuid());


//        keyed计数滑动窗口
        keyedStream.countWindow(10);


        env.execute();
    }
}
