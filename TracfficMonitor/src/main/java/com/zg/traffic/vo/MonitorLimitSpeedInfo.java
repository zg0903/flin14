package com.zg.traffic.vo;

import lombok.Data;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name com.zg.traffic.vo
 * @date 2022-09-12-9:54
 * @Desc:
 */
@Data
public class MonitorLimitSpeedInfo {
    private String areaId;
    private String roadId;
    private String monitorId;
    private Double limitSpeed;

    public MonitorLimitSpeedInfo(String areaId, String roadId, String monitorId, Double limitSpeed) {
        this.areaId = areaId;
        this.roadId = roadId;
        this.monitorId = monitorId;
        this.limitSpeed = limitSpeed;
    }
}
