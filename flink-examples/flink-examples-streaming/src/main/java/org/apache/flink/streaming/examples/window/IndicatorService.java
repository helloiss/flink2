package org.apache.flink.streaming.examples.window;

import redis.clients.jedis.Jedis;

import java.util.Map;

public class IndicatorService {

    private Jedis jedis = new Jedis("localhost", 6379); // 根据实际配置修改

    /**
     * 获取指标值，根据flink中间态结果累计
     *
     * @param event
     * @param metricConfig
     *
     * @return
     */
    public double getIndicatorValue(
            DynamicTransactionMetrics.MetricEvent event,
            DynamicTransactionMetrics.MetricConfig metricConfig) {
        String pk = metricConfig.getConfigName() + "_" + event.getPrimaryKey();
        long eventTime = event.getTimestamp();
        long windowSize = metricConfig.getWindowSize();
        long currentIndex = eventTime / metricConfig.getWindowSize();

        //TODO 测试窗口index的逻辑是否正确
        //TODO 根据计算类型类计算 当前实现逻辑为近XX个周期的值
        double indicatorValue = 0.0d;
        for (long i = 0; i < windowSize; i++) {
            String key = metricConfig.getConfigName() + event.getPrimaryKey() + (currentIndex+i);
            //优化读取逻辑
            double value = Double.valueOf(jedis.get(key));
            indicatorValue += value;
        }
        return indicatorValue;
    }


    private long calcWindowKey(long eventTime, long windowSize) {
        return (int) eventTime / windowSize;
    }


}
