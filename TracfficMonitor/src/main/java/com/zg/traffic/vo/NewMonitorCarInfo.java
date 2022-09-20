package com.zg.traffic.vo;

import lombok.Data;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name com.zg.traffic.vo
 * @date 2022-09-20-21:35
 * @Desc:
 */

@Data
public class NewMonitorCarInfo {
    private String areaId;
    private String roadId;
    private String monitorId;
    private String cameraId;
    private Long actionTime;
    private String car;
    private Double speed;
    private Double speedLimit;

    public NewMonitorCarInfo(String areaId, String roadId, String monitorId, String cameraId, Long actionTime, String car, Double speed, Double speedLimit) {
        this.areaId = areaId;
        this.roadId = roadId;
        this.monitorId = monitorId;
        this.cameraId = cameraId;
        this.actionTime = actionTime;
        this.car = car;
        this.speed = speed;
        this.speedLimit = speedLimit;
    }
}
