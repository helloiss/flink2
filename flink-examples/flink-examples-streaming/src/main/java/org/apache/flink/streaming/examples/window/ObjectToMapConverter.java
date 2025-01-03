package org.apache.flink.streaming.examples.window;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class ObjectToMapConverter {
	public static Map<String, Object> convertObjectToMap(Object obj) {
		Map<String, Object> map = new HashMap<>();

		// 检查对象是否为null
		if (obj == null) {
			return map;
		}

		// 获取对象的类
		Class<?> objClass = obj.getClass();

		// 获取对象的所有属性
		Field[] fields = objClass.getDeclaredFields();

		for (Field field : fields) {
			// 设置可访问私有属性
			field.setAccessible(true);
			try {
				// 获取属性名称和值
				String key = field.getName();
				Object value = field.get(obj);
				map.put(key, value);
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		return map;
	}
}
