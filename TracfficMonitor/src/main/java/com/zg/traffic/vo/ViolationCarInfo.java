package com.zg.traffic.vo;

import lombok.Data;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name com.zg.traffic.vo
 * @date 2022-09-14-21:54
 * @Desc:
 */

@Data
public class ViolationCarInfo {


    private String car;
    private String violation;
    private String createTime;
    private String detail;

    public ViolationCarInfo(String car, String violation, String createTime, String detail) {
        this.car = car;
        this.violation = violation;
        this.createTime = createTime;
        this.detail = detail;
    }


    @Override
    public String toString() {
        return "ViolationCarInfo{" +
                "car='" + car + '\'' +
                ", violation='" + violation + '\'' +
                ", createTime='" + createTime + '\'' +
                ", detail='" + detail + '\'' +
                '}';
    }
}
