package org.apache.flink.streaming.examples.window.config;

public class MetricConfig {

	private final String key; // 用于keyBy的字段
	private final long windowSize; // 窗口大小（毫秒）
	private final String calcType;

	public MetricConfig(String key, long windowSize, String calcType) {
		this.key = key;
		this.windowSize = windowSize;
		this.calcType = calcType;
	}

	public String getKey() {
		return key;
	}

	public long getWindowSize() {
		return windowSize;
	}

	public String getCalcType() {
		return calcType;
	}
}
