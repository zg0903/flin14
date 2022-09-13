package com.zg.traffic.vo;

import lombok.Data;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name com.zg.traffic.vo
 * @date 2022-09-13-21:20
 * @Desc:
 */

@Data
public class Top5MonitorInfo {

    private String windowStartTime;
    private String windowEndtime;
    private String monitorId;
    private Long highSpeedCarCount;
    private Long middleSpeedCarCount;
    private Long normalSpeedCarCount;
    private Long lowSpeedCarCount;

    public Top5MonitorInfo(String windowStartTime, String windowEndtime, String monitorId, Long highSpeedCarCount, Long middleSpeedCarCount, Long normalSpeedCarCount, Long lowSpeedCarCount) {
        this.windowStartTime = windowStartTime;
        this.windowEndtime = windowEndtime;
        this.monitorId = monitorId;
        this.highSpeedCarCount = highSpeedCarCount;
        this.middleSpeedCarCount = middleSpeedCarCount;
        this.normalSpeedCarCount = normalSpeedCarCount;
        this.lowSpeedCarCount = lowSpeedCarCount;
    }
}
