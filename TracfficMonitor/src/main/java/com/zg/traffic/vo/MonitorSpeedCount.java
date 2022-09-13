package com.zg.traffic.vo;

import lombok.Data;

import java.util.*;

import static java.util.Map.Entry.comparingByValue;
import static java.util.stream.Collectors.toMap;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name com.zg.traffic.vo
 * @date 2022-09-13-21:26
 * @Desc:
 */
@Data
public class MonitorSpeedCount implements Comparable<MonitorSpeedCount> {

    private Long xhighSpeedCarCount;
    private Long xmiddleSpeedCarCount;
    private Long xnormalSpeedCarCount;
    private Long xlowSpeedCarCount;

    public MonitorSpeedCount(Long xhighSpeedCarCount, Long xmiddleSpeedCarCount, Long xnormalSpeedCarCount, Long xlowSpeedCarCount) {
        this.xhighSpeedCarCount = xhighSpeedCarCount;
        this.xmiddleSpeedCarCount = xmiddleSpeedCarCount;
        this.xnormalSpeedCarCount = xnormalSpeedCarCount;
        this.xlowSpeedCarCount = xlowSpeedCarCount;
    }

    public void setXhighSpeedCarCountPlusOne() {
        this.xhighSpeedCarCount += 1;
    }

    public void setXmiddleSpeedCarCountPlusOne() {
        this.xmiddleSpeedCarCount += 1;
    }

    public void setXnormalSpeedCarCountPlusOne() {
        this.xnormalSpeedCarCount += 1;
    }

    public void setXlowSpeedCarCountPlusOne() {
        this.xlowSpeedCarCount += 1;
    }


    public static void main(String[] args) {
        MonitorSpeedCount m1 = new MonitorSpeedCount(2L, 0L, 0L, 0L);
        MonitorSpeedCount m2 = new MonitorSpeedCount(2L, 2L, 0L, 0L);
        MonitorSpeedCount m3 = new MonitorSpeedCount(3L, 0L, 0L, 0L);

//        ArrayList<MonitorSpeedCount> al = new ArrayList<>();
//        al.add(m1);
//        al.add(m2);
//        al.add(m3);
//
//        Collections.sort(al);
//        for (MonitorSpeedCount monitorSpeedCount : al) {
//            System.out.println(monitorSpeedCount);
//        }


        HashMap<String, MonitorSpeedCount> map = new HashMap<>();
        map.put("1",m1);
        map.put("2",m2);
        map.put("3",m3);
        map.forEach((k,v)-> System.out.println(k+v));


        LinkedHashMap<String, MonitorSpeedCount> collect = map.entrySet().stream().sorted(comparingByValue())
                .collect(
                        toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                                LinkedHashMap::new)
                );
        System.out.println("----------------");
        collect.forEach((k,v)-> System.out.println(k+v));






    }

    @Override
    public int compareTo(MonitorSpeedCount o) {
        if (this.getXhighSpeedCarCount() != o.getXhighSpeedCarCount()) {
            return this.getXhighSpeedCarCount() - o.getXhighSpeedCarCount() > 0L ? -1 : 1;
        } else {
            if (this.getXmiddleSpeedCarCount() != o.getXmiddleSpeedCarCount()) {
                return this.getXmiddleSpeedCarCount() - o.getXmiddleSpeedCarCount() > 0L ? -1 : 1;
            } else {
                if (this.getXnormalSpeedCarCount() != o.getXnormalSpeedCarCount()) {
                    return this.getXnormalSpeedCarCount() - o.getXnormalSpeedCarCount() > 0 ? -1 : 1;
                } else {
                    if (this.getXlowSpeedCarCount() != o.getXlowSpeedCarCount()) {
                        return this.getXlowSpeedCarCount() - o.getXlowSpeedCarCount() > 1 ? -1 : 1;
                    }
                }
            }
        }
        return 0;
    }


}
