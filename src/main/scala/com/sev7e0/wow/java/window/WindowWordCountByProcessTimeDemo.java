package com.sev7e0.wow.java.window;


import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.atomic.AtomicLong;


/**
 * 每五秒统计一次十秒内的word count
 *
 * 滑动窗口，当窗口大小大于滑动大小，窗口重叠
 * 			当窗口大小等于滑动大小，是连续的
 * 			当窗口大小小于滑动大小，数据丢失
 */
public class WindowWordCountByProcessTimeDemo {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> textStream = executionEnvironment
			.socketTextStream("localhost", 9090);

		SingleOutputStreamOperator<Tuple2<String,Long>> flatMap = textStream.flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
			@Override
			public void flatMap(String value, Collector<Tuple2<String,Long>> out) throws Exception {
				String[] strings = value.split(" ");
				for (String string : strings) {
					out.collect(Tuple2.of(string, 1L));
				}
			}
		});

		flatMap.keyBy(0)
			//窗口大小 10s 每次滑动5s
			.timeWindow(Time.seconds(10),Time.seconds(5))
			.process(new SumProcessWindowFunction())
			.print()
			.setParallelism(1);
		System.out.println(System.currentTimeMillis());

		executionEnvironment.execute(WindowWordCountByProcessTimeDemo.class.getName());

	}
	
}

/**
 *
 */
class SumProcessWindowFunction extends ProcessWindowFunction<Tuple2<String,Long>, Tuple2<String,Long>, Tuple, TimeWindow>{
	FastDateFormat dataFormat = FastDateFormat.getInstance("HH:mm:ss");


	@Override
	public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {
		System.out.println("当天系统的时间："+dataFormat.format(System.currentTimeMillis()));
		System.out.println("处理时间："+dataFormat.format(context.currentProcessingTime()));
		System.out.println("开始时间："+dataFormat.format(context.window().getStart()));
		System.out.println("结束时间："+dataFormat.format(context.window().getEnd()));
		AtomicLong count = new AtomicLong();
		elements.forEach(ele -> count.addAndGet(1L));
		out.collect(Tuple2.of(tuple.getField(0), count.get()));
	}
}
