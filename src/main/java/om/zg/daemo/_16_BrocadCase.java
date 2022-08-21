package om.zg.daemo;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name om.zg.daemo
 * @date 2022-08-20-22:18
 * @Desc:
 */
public class _16_BrocadCase {
    public static void main(String[] args) throws Exception {
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


//        s1 事件流
//        s2 维护维度信息  只会来一次 来的时间不一定  -》 利用广播流

        //s2->广播流
//        BroadcastStream<Tuple3<String, String, String>> s2BroadcastStream = s2.broadcast(new MapStateDescriptor<String, Tuple2<String, String>>("userInfoState", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
//        })));

        MapStateDescriptor<String, Tuple2<String, String>> userInfoStateDesc = new MapStateDescriptor<>("userInfoState", TypeInformation.of(String.class), TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        }));

        BroadcastStream<Tuple3<String, String, String>> s2BroadcastStream = s2.broadcast(userInfoStateDesc);

//        哪个流需要 就连接
        BroadcastConnectedStream<Tuple2<String, String>, Tuple3<String, String, String>> connected = s1.connect(s2BroadcastStream);

        connected.process(new BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>() {


//            BroadcastState<String, Tuple2<String, String>> userInfoBroadCastState;


                              //            处理左事件流
              /*
                 value 事件流中的一条数据
                 */
                              @Override
                              public void processElement(Tuple2<String, String> value, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
//                通过事件流中value的key获取广播流中的信息
                /*
                if (userInfoBroadCastState != null) {
                    Tuple2<String, String> userInfo = userInfoBroadCastState.get(value.f0);
                    out.collect(value.f0 + "," + value.f1 + "," + (userInfo.f0 == null ? null : userInfo.f0
                            + "," + (userInfo.f1 == null ? null : userInfo.f1);
                } else {
                    out.collect(value.f0 + "," + value.f1 + "," + null + "," + null);
                }
                */

                                  ReadOnlyBroadcastState<String, Tuple2<String, String>> broadcastState = ctx.getBroadcastState(userInfoStateDesc);
                                  if (broadcastState != null) {
                                      Tuple2<String, String> userInfo = broadcastState.get(value.f0);
                                      out.collect(value.f0 + "," + value.f1 + "," + (userInfo.f0 == null ? null : userInfo.f0)
                                              + "," + (userInfo.f1 == null ? null : userInfo.f1));
                                  } else {
                                      out.collect(value.f0 + "," + value.f1 + "," + null + "," + null);
                                  }


                              }

                              //            处理右广播流
              /*
                 value 广播流中的一条数据
                 */
                              @Override
                              public void processBroadcastElement(Tuple3<String, String, String> value, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.Context ctx, Collector<String> out) throws Exception {
//                获取广播状态 广播状态 可以理解为一个被flink管理的hashmap
                                  BroadcastState<String, Tuple2<String, String>> userInfoState = ctx.getBroadcastState(userInfoStateDesc);
/*
                if (userInfoBroadCastState == null) {
                    userInfoBroadCastState = ctx.getBroadcastState(userInfoStateDesc);
                }
//                将获得的这条广播流数据 拆分后 装入广播状态

                userInfoBroadCastState.put(value.f0, Tuple2.of(value.f1, value.f2));

            }
            */
                                  userInfoState.put(value.f0, Tuple2.of(value.f1, value.f2));
                              }
                          }
        ).print();


        env.execute();
    }
}