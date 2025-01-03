package org.apache.flink.streaming.examples.window;

import com.alibaba.fastjson2.JSONObject;

import java.util.List;

public class MetricConfigRepository {

	private volatile static List<DynamicTransactionMetrics.MetricConfig> metricConfigList;

	public static List<DynamicTransactionMetrics.MetricConfig> getLastedConfig(){
		return metricConfigList;
	}

	public static void updateCache(String config){
		List<DynamicTransactionMetrics.MetricConfig> list = JSONObject.parseObject(config,metricConfigList.getClass());
		metricConfigList = list;
	}

}
