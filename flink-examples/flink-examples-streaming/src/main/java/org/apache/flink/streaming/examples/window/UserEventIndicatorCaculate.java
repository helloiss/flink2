package org.apache.flink.streaming.examples.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class UserEventIndicatorCaculate {
    
    public static void main(String[] args) throws Exception {
        // 创建 Flink 执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        // 模拟输入数据流（交易事件，有用户ID、金额和时间戳）
        DataStream<Transaction> transactionStream = env.fromElements(
            new Transaction("user1", 100, System.currentTimeMillis()),
            new Transaction("user1", 150, System.currentTimeMillis() + 2000),
            new Transaction("user2", 200, System.currentTimeMillis() + 4000),
            new Transaction("user1", 250, System.currentTimeMillis() + 6000),
            new Transaction("user2", 300, System.currentTimeMillis() + 8000),
            new Transaction("user3", 150, System.currentTimeMillis() + 10000),
            new Transaction("user1", 300, System.currentTimeMillis() + 12000)
        );

		env.setParallelism(1);
        // 应用时间窗口并定义用户总额的聚合
        transactionStream
            .keyBy(transaction -> transaction.getUserId()) // 按照用户ID分组
			.timeWindow(Time.seconds(5)) // 创建一个5分钟的窗口
            .aggregate(new SumAggregate(), new WindowResult()) // 自定义聚合和窗口处理
            .print(); // 输出结果

		System.out.println("MultiElementWindowExample");
        // 启动 Flink 应用
        env.execute("Multi-element Window Example");
		Thread.sleep(100000);
    }

    // 定义交易事件类
    public static class Transaction implements Serializable {
        public String userId;
		public double amount;
		public long timestamp;

		public Transaction(){}

        public Transaction(String userId, double amount, long timestamp) {
            this.userId = userId;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        public String getUserId() {
            return userId;
        }

        public double getAmount() {
            return amount;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    // 自定义聚合函数：计算总金额
    public static class SumAggregate implements AggregateFunction<Transaction, Double, Double> {
        @Override
        public Double createAccumulator() {
            return 0.0; // 初始化为0
        }

        @Override
        public Double add(Transaction value, Double accumulator) {
            return accumulator + value.getAmount(); // 更新总金额
        }

        @Override
        public Double getResult(Double accumulator) {
            return accumulator; // 返回聚合结果
        }

        @Override
        public Double merge(Double a, Double b) {
            return a + b; // 合并两个累加器
        }
    }

    // 自定义窗口处理函数：输出窗口结果
    public static class WindowResult extends ProcessWindowFunction<Double, String, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Double> elements, Collector<String> out) {
            double sum = 0.0;
            // 计算窗口内的总金额
            for (Double element : elements) {
                sum += element; // 累加所有用户ID的交易总金额
            }


            long windowEnd = context.window().getEnd(); // 获取窗口结束时间
            out.collect("User: " + key + ", Total Amount: " + sum + ", Window End: " + windowEnd);
        }
    }
}
