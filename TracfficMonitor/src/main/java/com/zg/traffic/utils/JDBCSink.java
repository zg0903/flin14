package com.zg.traffic.utils;

import com.zg.traffic.vo.OverSpeedCarInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name com.zg.traffic.utils
 * @date 2022-09-12-14:49
 * @Desc:
 */
public class JDBCSink extends RichSinkFunction<OverSpeedCarInfo> {
    Connection coon = null;
    PreparedStatement pst = null;
    boolean flag = false;

    @Override
    public void open(Configuration parameters) throws Exception {
        coon = DriverManager
                .getConnection("jdbc:mysql://localhost:3306/traffic_monitor?characterEncoding=UTF-8&serverTimezone=GMT%2B8",
                        "root", "mima0903!");
        pst = coon.prepareStatement("insert into t_speeding_info(car,monitor_id,road_id,real_speed,limit_speed,action_time) values(?,?,?,?,?,?)");

    }

    @Override
    public void invoke(OverSpeedCarInfo value, Context context) throws Exception {
        pst.setString(1, value.getCar());
        pst.setString(2, value.getMonitorId());
        pst.setString(3, value.getRoadId());
        pst.setDouble(4, value.getRealSpeed());
        pst.setDouble(5, value.getLimitSpeed());
        pst.setLong(6, value.getActiopnTime());
        pst.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        pst.close();
        coon.close();
    }
}
