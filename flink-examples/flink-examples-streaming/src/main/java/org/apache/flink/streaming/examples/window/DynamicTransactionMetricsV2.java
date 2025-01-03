package org.apache.flink.streaming.examples.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson2.JSONObject;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DynamicTransactionMetricsV2 {

	private static final ExecutorService executorService = Executors.newFixedThreadPool(10);

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 假设有一个配置流
		DataStream<String> metricConfigStream = env.addSource(null);

		// 假设有一个用户交易事件流
		DataStream<MetricEvent> transactionStream = env.addSource(null);

		// 广播指标配置
		MapStateDescriptor<String, MetricConfig> configDescriptor = new MapStateDescriptor<>("MetricConfig", String.class, MetricConfig.class);

		BroadcastStream<MetricConfig> broadcastConfig = metricConfigStream.flatMap(new FlatMapFunction<String, MetricConfig>() {
			@Override
			public void flatMap(String value, Collector<MetricConfig> out) throws Exception {
				List<MetricConfig> configs = JSONObject.parseObject(value, List.class);
				for (MetricConfig config : configs) {
					out.collect(config);
				}
			}
		}).broadcast(configDescriptor);

		// 处理用户交易事件
		transactionStream
			.connect(broadcastConfig) // 连接广播状态
			.process(new TransactionMetricsProcessFunctionV2())
			.print();

		env.execute("Dynamic Transaction Metrics");
	}

	// 用户交易事件类
	public static class MetricEvent {
		private final String userId; // 用户ID
		private final long timestamp; // 交易时间戳
		private final double amount; // 交易金额

		public MetricEvent(String userId, String deviceId, long timestamp, double amount) {
			this.userId = userId;
			this.timestamp = timestamp;
			this.amount = amount;
		}

		public String getUserId() {
			return userId;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public double getAmount() {
			return amount;
		}
	}

	// 指标配置类
	public static class MetricConfig {
		private final String configName;
		private final String key; // 用于keyBy的字段（"userId"或"deviceId"）
		private final long windowSize; // 窗口大小（毫秒）

		public MetricConfig(String configName, String key, long windowSize) {
			this.configName = configName;
			this.key = key;
			this.windowSize = windowSize;
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
	}

	// 自定义处理函数
	public static class TransactionMetricsProcessFunctionV2 extends KeyedBroadcastProcessFunction<String, MetricEvent, MetricConfig, String> {

		// 描述符用于指标配置
		private MapStateDescriptor<String, MetricConfig> configDescriptor;
		private MapState<String, Double> cumulativeValues; // 保存每个event key的累计值
		private long windowSize; // 窗口大小

		@Override
		public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
			MapStateDescriptor<String, Double> descriptor = new MapStateDescriptor<>("cumulativeValues", String.class, Double.class);
			configDescriptor = new MapStateDescriptor<>("MetricConfig", String.class, MetricConfig.class);
			cumulativeValues = getRuntimeContext().getMapState(descriptor);
		}

		@Override
		public void processElement(MetricEvent event, KeyedBroadcastProcessFunction<String, MetricEvent, MetricConfig, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
			MetricConfig metricConfig = ctx.getBroadcastState(configDescriptor).get("MetricConfig");
			windowSize = metricConfig.getWindowSize();
			long currentTime = event.getTimestamp();
			// 计算当前时间所属的窗口序号
			long windowKey = (currentTime / windowSize) * windowSize; // 窗口起始时间
			String key = metricConfig.getConfigName() + event.getUserId() + windowKey;


			try {
				// 更新累计值，以 (key, windowKey) 组合为状态键 可以考虑使用一个乐观锁
				cumulativeValues.put(key + "_" + windowKey, cumulativeValues.get(key) + event.getAmount());
				// 输出当前累计值
				out.collect("Key: " + key + ", Window Start: " + windowKey + ", Cumulative Value: " + cumulativeValues.get(key));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void processBroadcastElement(MetricConfig value, KeyedBroadcastProcessFunction<String, MetricEvent, MetricConfig, String>.Context ctx, Collector<String> out) throws Exception {
			ctx.getBroadcastState(configDescriptor).put("MetricConfig",value);
		}
	}
}
