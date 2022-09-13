package com.zg.traffic.utils;

import org.apache.commons.math3.random.GaussianRandomGenerator;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

import java.text.ParseException;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name com.zg.traffic.utils
 * @date 2022-09-08-21:53
 * @Desc: 1模拟数据生成到文件中 2 模拟数据神生成到 Kafka中 3
 */
public class MakeData {
    //    区域id  道路id 卡扣id, 摄像头id, 拍摄时间 车辆信息  车辆速度
    public static void main(String[] args) throws FileNotFoundException, ParseException {

        PrintWriter pw = new PrintWriter("trafficdata");
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092,");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("buffer.memory", 33554432);//32m
        props.put("batch.size", 16384);//16k
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        Random random = new Random();


        for (int i = 0; i < 30000; i++) {
//       模拟车
            String car = radomPlate();
//            高斯分布 每天通过卡扣数大部分不超过100个卡扣
            int throuldMonitorCount = getThrouldMonitor();

//            模拟车通过的卡口
            for (int j = 0; j < 100; j++) {
//                通过的区域
                String areaId = String.format("%02d", random.nextInt(8));
//                通过的道路
                String roadId = String.format("%02d", random.nextInt(50));
//                通过的卡扣
                String monitorId = String.format("%04d", random.nextInt(9999));
//                通过的摄像头
                String cameraId = String.format("%06d", random.nextInt(9999));
//                摄像头拍摄时间
                String randomActionTime = getCurrentDate() + " " + getRandomHour() + ":" + getRandomMinOrSec() + ":" + getRandomMinOrSec();
                SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date parse = sd.parse(randomActionTime);
                long actionTime = parse.getTime();

//                拍摄车辆速度
                double speed = Double.parseDouble(getCarSpeed());

                String info = areaId + "\t" + roadId + "\t" + monitorId + "\t" + cameraId + "\t" + actionTime + "\t" + car + "\t" + speed;
//                pw.write(info);
                System.out.println(info);

                producer.send(new ProducerRecord<String, String>("traffic-topic", info));


            }

        }
        pw.close();
        producer.close();


    }


    public static String radomPlate() {
        String plateNO = "";
        //省号
        String[] sheng = {"京", "津", "沪", "渝", "冀", "吉", "辽", "黑", "湘", "鄂", "甘", "晋", "陕", "豫", "川", "云", "桂",
                "蒙", "贵", "青", "藏", "新", "宁", "粤", "琼", "闽", "苏", "浙", "赣", "鲁", "皖"};
        int i = (int) (Math.random() * (sheng.length));
        plateNO += sheng[i];
        //字母
        plateNO += getChater();
        //字母总数
        int Anum = (int) (Math.random() * 3);
        Set<Integer> set = new HashSet<>();
        while (set.size() != Anum) {
            set.add((int) (Math.random() * 5));
        }
        //插入编号
        for (int k = 0; k < 5; k++) {
            if (set.contains(k)) {
                plateNO += getChater();
            } else {
                plateNO += (int) (Math.random() * 10);
            }
        }
        return plateNO;
    }

    //获取一个随机字母
    public static String getChater() {
        int j = 0;
        do {
            j = Integer.valueOf('A') + (int) (Math.random() * 26);
        }
        while ((j == 73) || (j == 79));
        return "" + (char) j;
    }


    public static int getThrouldMonitor() {
        GaussianRandomGenerator gaussianRandomGenerator = new GaussianRandomGenerator(new JDKRandomGenerator());
        double v = gaussianRandomGenerator.nextNormalizedDouble() * 30;
        return Math.abs((int) v);
    }

    public static String getCarSpeed() {
        GaussianRandomGenerator gaussianRandomGenerator = new GaussianRandomGenerator(new JDKRandomGenerator());
        double v = gaussianRandomGenerator.nextNormalizedDouble() * 60;
        return String.format("%.2f", Math.abs(v));
    }


    public static String getCurrentDate() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return simpleDateFormat.format(new Date()).toString();
    }

    public static String getRandomHour() {
        Random random = new Random();
        int i = random.nextInt(24);
        return String.format("%02d", i);
    }

    public static String getRandomMinOrSec() {
        Random random = new Random();
        int i = random.nextInt(60);
        return String.format("%02d", i);
    }


}
