package com.lhs.flink.example.java.p2_watermark;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.text.Format;
import java.text.SimpleDateFormat;

/**
 *
 * AssignerWithPeriodicWatermarks   周期性的 watermark
 * @author lihuasong
 * @description 描述
 * @create 2019/8/6
 **/
public class MyTimestampWaterMarks implements AssignerWithPeriodicWatermarks<Tuple2<String,Long>> {

    Logger logger = LoggerFactory.getLogger(MyTimestampWaterMarks.class);

    private long maxOutOfOrderness;  // 最大延迟的时间
    private long currentMaxTimestamp;
    private Watermark watermark;

    Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public MyTimestampWaterMarks(long maxOutOfOrderness){
        this.maxOutOfOrderness = maxOutOfOrderness;
    }

    /**
     * 获取当前的水印：  用当前的时间  减去  可以接受的最大延迟
     *
     * 水印表示接受延迟的最后时间
     * @return
     */
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        return watermark;
    }


    /**
     * 这个是提取日志中的时间戳
     * @param kv
     * @param l
     * @return
     */
    @Override
    public long extractTimestamp(Tuple2<String, Long> kv, long l) {
        Long eventTime = kv.f1;
        currentMaxTimestamp = Math.max(eventTime,currentMaxTimestamp);
        System.out.println("event_timestamp ["+kv+"|"+format.format(kv.f1)+"], current:["+currentMaxTimestamp+"|"+format.format(currentMaxTimestamp)+"],watermark:["+watermark.toString()+"]");
        return eventTime;
    }
}
