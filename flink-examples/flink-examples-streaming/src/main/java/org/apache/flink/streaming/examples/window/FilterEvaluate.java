package org.apache.flink.streaming.examples.window;

import org.mvel2.MVEL;
import java.util.Map;

public class FilterEvaluate {
    public static boolean evaluateFilterCondition(String condition, Map<String, Object> valueMap) {
        // 如果没有过滤条件，则直接返回 true
        if (condition == null || condition.isEmpty()) {
            return true;
        }
        return (Boolean) MVEL.eval(condition, valueMap);
    }
}
