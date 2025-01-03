package org.apache.flink.streaming.examples.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson2.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class DynamicTransactionMetrics {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 假设有一个配置流
		DataStream<String> metricConfigStream = env.addSource(null);

		// 假设有一个用户交易事件流
		DataStream<OriginEvent> transactionStream = env.addSource(null);

		DataStream<MetricEvent> metricEventDataStream = transactionStream.flatMap((FlatMapFunction<OriginEvent, MetricEvent>) (value, out) -> {
			//动态加载配置
			List<MetricConfig> metricConfigs = MetricConfigRepository.getLastedConfig();
			for (MetricConfig metricConfig : metricConfigs) {
				Map<String, Object> valueMap = ObjectToMapConverter.convertObjectToMap(value);
				String pk = metricConfig.getConfigName() + "_" + valueMap.get(metricConfig.getKey()).toString();
				out.collect(new MetricEvent(pk, value.timestamp, value.amount, metricConfig.getConfigName(), valueMap.get(metricConfig.getObject()).toString()));
			}
		});

		// 广播指标配置
		MapStateDescriptor<String, String> configDescriptor = new MapStateDescriptor<>("MetricConfig", String.class, String.class);

		BroadcastStream<String> broadcastConfig = metricConfigStream.broadcast(configDescriptor);


		// 处理用户交易事件
		metricEventDataStream.keyBy(MetricEvent::getPrimaryKey)
			.connect(broadcastConfig) // 连接广播状态
			.process(new TransactionMetricsProcessFunction())
			.print();

		env.execute("Dynamic Transaction Metrics");
	}

	public static class OriginEvent {
		private String userId;
		private String deviceId;
		private String ip;
		private long timestamp; // 交易时间戳
		private double amount; // 交易金额

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
		private final String primaryKey; // 用户ID
		private final long timestamp; // 交易时间戳
		private final double amount; // 交易金额
		private String metricName;
		//客体
		private String object;

		public MetricEvent(String userId, long timestamp, double amount, String metricName, String object) {
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

	// 指标配置类
	public static class MetricConfig {
		private final String configName;
		private final String key; // 用于keyBy的字段（"userId"或"deviceId"）
		private final long windowSize; // 窗口大小（毫秒）
		private final String object;//客体字段
		private final String calcType;

		public MetricConfig(String configName, String key, long windowSize, String object, String calcType) {
			this.configName = configName;
			this.key = key;
			this.windowSize = windowSize;
			this.object = object;
			this.calcType = calcType;
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
		private MapState<String, HashSet<String>> countDistinctMap; // 存储每个 Metric primaryKey 对应的 unique object 集合

		private long windowSize; // 窗口大小

		private transient Jedis jedis; // Redis 客户端

		@Override
		public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
			MapStateDescriptor<String, Double> descriptor = new MapStateDescriptor<>("cumulativeValues", String.class, Double.class);
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
			// 初始化 Redis 连接
			jedis = new Jedis("localhost", 6379); // 根据实际配置修改
		}

		@Override
		public void processElement(MetricEvent event, KeyedBroadcastProcessFunction<String, MetricEvent, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
			String currentConfig = ctx.getBroadcastState(configDescriptor).get("metricConfig");
			List<MetricConfig> metricConfigs = JSONObject.parseObject(currentConfig, List.class);
			MetricConfig config = null;

			for (MetricConfig c : metricConfigs) {
				if (c.getConfigName().equals(event.getMetricName())) {
					config = c;
					break;
				}
			}
			windowSize = config.getWindowSize();
			long currentTime = event.getTimestamp();
			// 计算当前时间所属的窗口序号
			long windowKey = (currentTime / windowSize) * windowSize; // 窗口起始时间
			String key = config.getConfigName() + event.getPrimaryKey() + windowKey;
			try {
				if (config.getCalcType().equals("COUNT DISTINCT")) {
					// 跟踪 unique object 的个数
					HashSet<String> objects = countDistinctMap.get(event.getPrimaryKey());
					if (objects == null) objects = new HashSet<>();
					objects.add(event.getObject()); // 添加当前 object
					countDistinctMap.put(key, objects);
					// 计算 unique object 的数量
					long uniqueCount = objects.size();
					// 存储到 Redis
					jedis.set(key, String.valueOf(uniqueCount));
				} else if (config.getCalcType().equals("SUM")) {
					// 更新累计值，以 (key, windowKey) 组合为状态键 可以考虑使用一个乐观锁
					cumulativeValues.put(key + "_" + windowKey, cumulativeValues.get(key) + event.getAmount());
					// 输出当前累计值
					// 存储到 Redis
					jedis.set(key + "_" + windowKey, String.valueOf(cumulativeValues.get(key)));
					out.collect("Key: " + key + ", Window Start: " + windowKey + ", Cumulative Value: " + cumulativeValues.get(key));
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void processBroadcastElement(String newConfig, Context ctx, Collector<String> out) throws Exception {
			// 处理新的配置，更新窗口大小等参数
			ctx.getBroadcastState(configDescriptor).put("metricConfig", newConfig);
			MetricConfigRepository.updateCache(newConfig);
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
