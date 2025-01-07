package org.apache.flink.streaming.examples.window;

import com.alibaba.fastjson.JSON;

import com.alibaba.fastjson2.TypeReference;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson2.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Consumer;

public class DynamicTransactionMetrics {

    //flink只计算中间状态数据,最终指标结果数据通过读取的时候累计中间数据完成
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        MetricConfig config = new MetricConfig(
                "test",
                "userId",
                10000l,
                "amount",
                "SUM",
                "RECENT",
                10l);
        String[] configs = new String[]{JSONObject.toJSONString(config)};
        List<MetricConfig> metricConfigs = new ArrayList<>();
        metricConfigs.add(config);

        MetricConfigRepository.updateCache(JSON.toJSONString(metricConfigs));
        // 假设有一个配置流
        DataStream<String> metricConfigStream = env.fromElements(configs);

        OriginEvent[] originEvents = new OriginEvent[]{
                new OriginEvent("user1", "device1", System.currentTimeMillis(), 100),
                new OriginEvent("user1", "device1", System.currentTimeMillis() + 1000, 100),
                new OriginEvent("user1", "device1", System.currentTimeMillis() + 3000, 100),
                new OriginEvent("user1", "device1", System.currentTimeMillis() + 5000, 100),
                new OriginEvent("user1", "device1", System.currentTimeMillis() + 8000, 100),
                new OriginEvent("user1", "device1", System.currentTimeMillis() + 10000, 100),
                new OriginEvent("user2", "device1", System.currentTimeMillis() + 15000, 100),
                new OriginEvent("user2", "device1", System.currentTimeMillis() + 16000, 100)
        };

        // 假设有一个用户交易事件流
        DataStream<OriginEvent> transactionStream = env.fromElements(originEvents);

        DataStream<MetricEvent> metricEventDataStream = transactionStream
                .flatMap((FlatMapFunction<OriginEvent, MetricEvent>) (value, out) -> {
                    //动态加载配置
                    List<MetricConfig> metricConfigCache = MetricConfigRepository.getLastedConfig();
                    for (MetricConfig metricConfig : metricConfigCache) {
                        Map<String, Object> valueMap = ObjectToMapConverter.convertObjectToMap(value);
                        String pk = valueMap.get(metricConfig.getKey()).toString();
                        out.collect(new MetricEvent(
                                pk,
                                value.timestamp,
                                value.amount,
                                metricConfig.getConfigName(),
                                valueMap.get(metricConfig.getObject()).toString()));
                    }
                })
                .returns(TypeInformation.of(new TypeHint<MetricEvent>() {
                }));

        // 广播指标配置
        MapStateDescriptor<String, String> configDescriptor = new MapStateDescriptor<>(
                "MetricConfig",
                String.class,
                String.class);

        BroadcastStream<String> broadcastConfig = metricConfigStream.broadcast(configDescriptor);


        // 处理用户交易事件
        metricEventDataStream.keyBy(MetricEvent::getPrimaryKey)
                .connect(broadcastConfig) // 连接广播状态
                .process(new TransactionMetricsProcessFunction())
                .print();

        env.execute("Dynamic Transaction Metrics");
        Thread.sleep(30 * 60 * 1000* 1000);
    }

    public static class OriginEvent {
        public String userId;
        public String deviceId;
        public String ip;
        public long timestamp; // 交易时间戳
        public double amount; // 交易金额

        public OriginEvent(String userId, String deviceId, long timestamp, double amount) {
            this.userId = userId;
            this.deviceId = deviceId;
            this.timestamp = timestamp;
            this.amount = amount;
        }

        public String getUserId() {
            return userId;
        }

        public String getDeviceId() {
            return deviceId;
        }

        public String getIp() {
            return ip;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public double getAmount() {
            return amount;
        }


    }

    // 用户交易事件类
    public static class MetricEvent {
        public final String primaryKey; // 用户ID
        public final long timestamp; // 交易时间戳
        public final double amount; // 交易金额
        public String metricName;
        //客体
        public String object;

        public MetricEvent(
                String userId,
                long timestamp,
                double amount,
                String metricName,
                String object) {
            this.primaryKey = userId;
            this.timestamp = timestamp;
            this.amount = amount;
            this.metricName = metricName;
            this.object = object;
        }

        public String getPrimaryKey() {
            return primaryKey;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public double getAmount() {
            return amount;
        }

        public String getMetricName() {
            return metricName;
        }

        public String getObject() {
            return object;
        }
    }

    // 指标配置类  最多保留300个时间切片
    public static class MetricConfig {
        public final String configName;
        public final String key; // 用于keyBy的字段（"userId"或"deviceId"）
        public final long windowSize; // 窗口切片大小（秒/分钟/小时/天）
        public final long windowLength; // 累计窗口长度
        public final String windowType; //窗口类型   本(当前滚动时间窗口对应的）/近（精确）
        public final String object;  //客体字段
        public final String calcType;

        public MetricConfig(
                String configName,
                String key,
                long windowSize,
                String object,
                String calcType,
                String windowType,
                long windowLength) {
            this.configName = configName;
            this.key = key;
            this.windowSize = windowSize;
            this.windowLength = windowLength;
            this.object = object;
            this.calcType = calcType;
            this.windowType = windowType;
        }

        public String getKey() {
            return key;
        }

        public long getWindowSize() {
            return windowSize;
        }

        public String getConfigName() {
            return configName;
        }

        public String getObject() {
            return object;
        }

        public String getCalcType() {
            return calcType;
        }
    }

    // 自定义处理函数
    public static class TransactionMetricsProcessFunction extends KeyedBroadcastProcessFunction<String, MetricEvent, String, String> {

        // 描述符用于指标配置
        private MapStateDescriptor<String, String> configDescriptor;
        private MapState<String, Double> cumulativeValues; // 保存每个event key的累计值

        //用于count distinct
        private MapState<String, HashSet<String>> countDistinctMap; // 存储每个 Metric_primaryKey 对应的 unique object 集合

        //滑动窗口数据
        private MapState<String, List<MetricEvent>> slidingWindowData;

//        private long windowSize; // 窗口大小，最小切片单位

        private transient Jedis jedis; // Redis 客户端

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            MapStateDescriptor<String, Double> descriptor = new MapStateDescriptor<>(
                    "cumulativeValues",
                    String.class,
                    Double.class);
            MapStateDescriptor<String, HashSet<String>> countDistinctMapDescriptor =
                    new MapStateDescriptor<>(
                            "countDistinctMap",
                            TypeInformation.of(String.class),
                            TypeInformation.of(new TypeHint<HashSet<String>>() {
                            })
                    );

            configDescriptor = new MapStateDescriptor<>("MetricConfig", String.class, String.class);
            cumulativeValues = getRuntimeContext().getMapState(descriptor);
            countDistinctMap = getRuntimeContext().getMapState(countDistinctMapDescriptor);

            MapStateDescriptor<String, List<MetricEvent>> slidingWindowDataDescriptor = new MapStateDescriptor<>(
                    "slidingWindowData",
                    TypeInformation.of(String.class),
                    TypeInformation.of(new TypeHint<List<MetricEvent>>() {
                    })
            );

            // 初始化 Redis 连接
//            jedis = new Jedis("localhost", 6379); // 根据实际配置修改

        }

        @Override
        public void processElement(
                MetricEvent event,
                KeyedBroadcastProcessFunction<String, MetricEvent, String, String>.ReadOnlyContext ctx,
                Collector<String> out) throws Exception {
            String currentConfig = ctx.getBroadcastState(configDescriptor).get("metricConfig");
            List<MetricConfig> metricConfigs = JSONObject.parseObject(
                    currentConfig,
                    new TypeReference<List<MetricConfig>>() {
                    });
            MetricConfig config = null;

            //TODO 优化
            for (MetricConfig c : metricConfigs) {
                if (c.getConfigName().equals(event.getMetricName())) {
                    config = c;
                    break;
                }
            }
            long windowSize = config.getWindowSize();
            long currentTime = event.getTimestamp();
            // 计算当前时间所属的窗口序号

            long windowStart =TimeWindow.getWindowStartWithOffset(currentTime,0L, windowSize);
            long windowKey = windowStart; // 窗口起始时间
            String key = config.getConfigName() +"_"+ event.getPrimaryKey() + "_" + windowKey;
            try {
                if (config.getCalcType().equals("COUNT DISTINCT")) {
                    // 跟踪 unique object 的个数
                    HashSet<String> objects = countDistinctMap.get(event.getPrimaryKey());
                    if (objects == null) objects = new HashSet<>();
                    objects.add(event.getObject()); // 添加当前count客体 object
                    countDistinctMap.put(key, objects);
                    // 计算 unique object 的数量
                    long uniqueCount = objects.size();
                    out.collect("Key: " + key + ", Window Start: " + windowKey + ", uniqueCount: " + uniqueCount);

//                    // 存储到 Redis
//                    jedis.set(key, String.valueOf(uniqueCount));
//                    jedis.set(key + "_events", objects.toString());
                    System.out.println("key=" + String.valueOf(uniqueCount));
                } else if (config.getCalcType().equals("SUM")) {
                    // 更新累计值，以 (key, windowKey) 组合为状态键 可以考虑使用一个乐观锁
                    if(cumulativeValues.get(key) == null) {
                        cumulativeValues.put(key, event.getAmount());
                    }else {
                        cumulativeValues.put(key, cumulativeValues.get(key) + event.getAmount());
                    }
                    // 输出当前累计值
                    // 存储到 Redis
//                    jedis.set(key, String.valueOf(cumulativeValues.get(key)));
                    System.out.println(
                            "Key: " + key + ", Window Start: " + windowKey + ", Cumulative Value: "
                                    + cumulativeValues.get(key));
                    out.collect(
                            "Key: " + key + ", Window Start: " + windowKey + ", Cumulative Value: "
                                    + cumulativeValues.get(key));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * 时间切片的index
         *
         * @param windowSize s=1000ms   m=1000ms* 60  h=60*m d=24h
         *
         * @return
         */
        private long calcWindowKey(long eventTime, long windowSize) {
            return (int) eventTime / windowSize;
        }


        @Override
        public void processBroadcastElement(
                String newConfig,
                Context ctx,
                Collector<String> out) throws Exception {
            // 处理新的配置，更新窗口大小等参数
            ctx.getBroadcastState(configDescriptor).put("metricConfig", newConfig);
            MetricConfigRepository.updateCache(newConfig);
        }

        private void cleanUpWindowData(List<MetricEvent> windowData, long currentTime, int windowSize) {
            // 只需检查列表的第一个元素
            while (!windowData.isEmpty()
                    && (currentTime - windowData.get(0).getTimestamp()) > windowSize) {
                windowData.remove(0); // 移除过期事件
            }
        }


        @Override
        public void close() throws Exception {
            // 关闭 Redis 连接
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}
