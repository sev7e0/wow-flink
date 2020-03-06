package com.sev7e0.wow.java.window;

import com.sev7e0.wow.java.utils.TestSourceUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicLong;

public class WindowProcessTimeByDisorderDemo {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> disorder = environment.addSource(new TestSourceUtils("disorder"));

		SingleOutputStreamOperator<Tuple2<String, Long>> flatMap = disorder.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
			@Override
			public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
				String[] split = value.split(",");
				out.collect(Tuple2.of(split[0], Long.parseLong(split[1])));
			}
		});


		flatMap.assignTimestampsAndWatermarks(new TimeExtractor())
			.keyBy(0)
			.timeWindow(Time.seconds(1),Time.seconds(1))
			.process(new SumProcessWindowFunctionByDisorder())
			.print()
			.setParallelism(1);

		environment.execute(WindowProcessTimeByDisorderDemo.class.getName());
	}
}

/**
 * 时间解析
 */
class TimeExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String,Long>>{

	@Nullable
	@Override
	public Watermark getCurrentWatermark() {
		return new Watermark(System.currentTimeMillis());
	}

	@Override
	public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
		return element.f1;
	}
}

/**
 * 处理逻辑
 */

class SumProcessWindowFunctionByDisorder extends ProcessWindowFunction<Tuple2<String,Long>, Tuple2<String,Long>, Tuple, TimeWindow> {
	@Override
	public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Tuple2<String, Long>> out) throws Exception {
		AtomicLong count = new AtomicLong();
		elements.forEach(ele -> count.addAndGet(1L));
		out.collect(Tuple2.of(tuple.getField(0), count.get()));
	}
}