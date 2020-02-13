package com.sev7e0.wow.java;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingDataProcessWithParallelDataSource {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<Long> dataStreamSource = environment.addSource(new ParallelSource()).setParallelism(2);

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


		filter.print().setParallelism(2);

		environment.execute(StreamingDataProcessWithParallelDataSource.class.getName());


	}
}
