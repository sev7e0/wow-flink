package com.sev7e0.wow.java.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class StreamingSourceFromCollection {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		List<String> list = Arrays.asList("Spark", "Hadoop", "Flink");

		//get datasource from collection
		DataStreamSource<String> collectionStreamingSource = environment.fromCollection(list);

		//map operator handle collection datasource
		SingleOutputStreamOperator<String> mapSource = collectionStreamingSource.map((MapFunction<String, String>) s -> "wow_" + s);

		//print result and set parallelism to 1
		mapSource.print().setParallelism(1);

		//start execute
		environment.execute(StreamingSourceFromCollection.class.getName());
	}
}
