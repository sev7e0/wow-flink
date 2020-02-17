package com.sev7e0.wow.java.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Custom single data source practice
 */
public class StreamingDataProcessWithCustomDataSource {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Long> dataStreamSource = environment.addSource(new SingleSource()).setParallelism(1);

		SingleOutputStreamOperator<Long> map = dataStreamSource.map(new MapFunction<Long, Long>() {
			@Override
			public Long map(Long aLong) throws Exception {
				System.out.println("receive data " + aLong);
				return aLong;
			}
		});

		SingleOutputStreamOperator<Long> filter = map.filter(new FilterFunction<Long>() {
			@Override
			public boolean filter(Long aLong) throws Exception {
				return aLong % 2 == 0;
			}
		});


		filter.print().setParallelism(1);

		environment.execute(StreamingDataProcessWithCustomDataSource.class.getName());


	}
}
