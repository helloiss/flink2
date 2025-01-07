package org.apache.flink.streaming.examples.window;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestWindow {

    public static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {



        DynamicTransactionMetrics.OriginEvent[] originEvents = new DynamicTransactionMetrics.OriginEvent[]{
                new DynamicTransactionMetrics.OriginEvent("user1", "device1", getTime("2025-01-06 00:00:00"), 100),
                new DynamicTransactionMetrics.OriginEvent("user1", "device1", getTime("2025-01-06 00:01:00"), 100),
                new DynamicTransactionMetrics.OriginEvent("user1", "device1", getTime("2025-01-06 00:01:20"), 100),
                new DynamicTransactionMetrics.OriginEvent("user1", "device1", getTime("2025-01-06 00:03:00"), 100),
                new DynamicTransactionMetrics.OriginEvent("user1", "device1", getTime("2025-01-06 00:03:30"), 100),
                new DynamicTransactionMetrics.OriginEvent("user1", "device1", getTime("2025-01-06 00:03:50"), 100),
                new DynamicTransactionMetrics.OriginEvent("user2", "device1", getTime("2025-01-06 00:05:15"), 100),
                new DynamicTransactionMetrics.OriginEvent("user2", "device1", getTime("2025-01-06 00:05:16"), 100)
        };

        long offset = 0l;

        for (DynamicTransactionMetrics.OriginEvent originEvent : originEvents) {
            int windowKey = (int) originEvent.getTimestamp()/ (5 * 60*1000);
            long start = TimeWindow.getWindowStartWithOffset(originEvent.getTimestamp(),offset, 5*60*1000);

            System.out.println(originEvent.getUserId()+"_" + windowKey  +": "+ simpleDateFormat.format(new Date(start)));
        }


    }

    public  static long getTime(String time){
        try {
            return simpleDateFormat.parse(time).getTime();
        } catch (ParseException e) {
            return System.currentTimeMillis();
        }
    }

}
