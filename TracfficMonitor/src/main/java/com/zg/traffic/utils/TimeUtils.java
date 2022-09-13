package com.zg.traffic.utils;

import java.sql.Date;
import java.text.SimpleDateFormat;

/**
 * @author zhuguang
 * @Project_name flink14
 * @Package_name com.zg.traffic.utils
 * @date 2022-09-13-22:45
 * @Desc:
 */
public class TimeUtils {


    public static String timeStampToDataStr(Long timeStamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
        String format = sdf.format(new Date(timeStamp));
        return format;
    }
}
