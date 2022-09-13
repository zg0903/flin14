package com.zg.traffic.vo;

import lombok.Data;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name com.zg.traffic.vo
 * @date 2022-09-12-11:07
 * @Desc:
 */

@Data
public class OverSpeedCarInfo {
    private String car;
    private String monitorId;

    private String roadId;
    private Double realSpeed;
    private Double limitSpeed;
    private Long actiopnTime;


    public OverSpeedCarInfo(String car, String monitorId, String roadId, Double realSpeed, Double limitSpeed, Long actiopnTime) {
        this.car = car;
        this.monitorId = monitorId;
        this.roadId = roadId;
        this.realSpeed = realSpeed;
        this.limitSpeed = limitSpeed;
        this.actiopnTime = actiopnTime;
    }
}
