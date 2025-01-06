package org.apache.flink.streaming.examples.window;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.TypeReference;

import java.util.ArrayList;
import java.util.List;

public class MetricConfigRepository {

	private volatile static List<DynamicTransactionMetrics.MetricConfig> metricConfigList = new ArrayList<>();

	public static List<DynamicTransactionMetrics.MetricConfig> getLastedConfig(){
		return metricConfigList;
	}

	public static void updateCache(String config){
		List<DynamicTransactionMetrics.MetricConfig> list = JSONObject.parseObject(config, new TypeReference<List<DynamicTransactionMetrics.MetricConfig>>(){});
		metricConfigList = list;
	}

}
