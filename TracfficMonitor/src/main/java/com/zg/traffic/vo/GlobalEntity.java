package com.zg.traffic.vo;

import lombok.Data;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name com.zg.traffic.vo
 * @date 2022-09-12-9:43
 * @Desc:kafka数据vo类
 */

@Data
public class GlobalEntity {

    private String areaId;
    private String roadId;
    private String monitorId;
    private String cameraId;
    private Long actionTime;
    private String car;
    private Double speed;

    public GlobalEntity(String areaId, String roadId, String monitorId, String cameraId, Long actionTime, String car, Double speed) {
        this.areaId = areaId;
        this.roadId = roadId;
        this.monitorId = monitorId;
        this.cameraId = cameraId;
        this.actionTime = actionTime;
        this.car = car;
        this.speed = speed;
    }
}
