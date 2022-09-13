package com.zg.traffic.vo;

import lombok.Data;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name com.zg.traffic.vo
 * @date 2022-09-12-16:55
 * @Desc:
 */

@Data
public class MonitorAvgSpeedInfo {

    private String windwStartTime;
    private String windwEndTime;
    private String monitorId;
    private Double aveSpeed;
    private Long carCount;

    public MonitorAvgSpeedInfo(String windwStartTime, String windwEndTime, String monitorId, Double aveSpeed, Long carCount) {
        this.windwStartTime = windwStartTime;
        this.windwEndTime = windwEndTime;
        this.monitorId = monitorId;
        this.aveSpeed = aveSpeed;
        this.carCount = carCount;
    }
}
