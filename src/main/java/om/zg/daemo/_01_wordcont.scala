package om.zg.daemo

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}


/**
 * @Project_name flink14
 * @Package_name om.zg.daemo
 * @author zhuguang
 * @date 2022-08-18-22:20
 * @Desc:
 */
object _01_wordcont {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val sourceStream = env.socketTextStream("localhost", 9999)

    sourceStream.flatMap(r => r.split("\t")).map(w => (w, 1))
      .keyBy(_._1).sum(1).print()
    //    sourceStream.flatMap(_.split("\\s+").map((_, 1)))

    env.execute()

  }

}
