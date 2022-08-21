package om.zg.daemo;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.daemo
 * @date 2022-08-21-10:40
 * @Desc:
 */
public class _17_ProcessFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        id 用户
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Tuple2<String, String>> s1 = stream1.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.of(arr[0], arr[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {
        });


//        id 年龄 城市
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9998);
        SingleOutputStreamOperator<Tuple3<String, String, String>> s2 = stream2.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1], arr[2]);
        }).returns(new TypeHint<Tuple3<String, String, String>>() {
        });


//        1 process  底层算子 自定义
        stream1.process(new ProcessFunction<String, Tuple2<String, String>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                RuntimeContext runtimeContext = getRuntimeContext();
                Runtime runtime = Runtime.getRuntime();

                super.open(parameters);
            }

            @Override
            public void processElement(String value, ProcessFunction<String, Tuple2<String, String>>.Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
//                可以测流输出
//                ctx.output(new OutputTag<>());

                String[] split = value.split(",");
                out.collect(Tuple2.of(split[0], split[1]));
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });


//        KeyedProcessFunction(key,数据类型,输出)
        KeyedStream<Tuple2<String, String>, String> keyedStream = s1.keyBy(r -> r.f0);
        keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<Integer, String>>() {
            @Override
            public void processElement(Tuple2<String, String> value, KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<Integer, String>>.Context ctx, Collector<Tuple2<Integer, String>> out) throws Exception {
                out.collect(Tuple2.of(Integer.valueOf(value.f0), value.f1.toUpperCase()));
            }
        });


//       processWindowFunction
//        pricessAllwindowFunction
//        coProcessFunction
//        orocessJoinFunction
//        brocastProcessFunction
//        keyedBroadcastProcessFunction


    }
}
