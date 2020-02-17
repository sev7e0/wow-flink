package com.sev7e0.wow.java.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Transformations {
	public static void main(String[] args) throws Exception{
		String host = "localhost";
		int port = 9090;
		try {
			 host = ParameterTool.fromArgs(args).get("host", "localhost");
			 port = ParameterTool.fromArgs(args).getInt("port",9090);
		}catch (Exception e){
			System.out.println("please use --host and --port");
		}


		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> streamSource = environment.socketTextStream(host, port).setParallelism(1);

		SingleOutputStreamOperator<WordCount> flatMap = streamSource.flatMap(new FlatMapFunction<String, WordCount>() {
			@Override
			public void flatMap(String s, Collector<WordCount> collector) throws Exception {
				String[] s1 = s.split(" ");
				for (String ss : s1){
					collector.collect(new WordCount(ss,1));
				}
			}
		});

		flatMap.keyBy("word").sum("count").print().setParallelism(1);
		environment.execute(Transformations.class.getName());

	}
	public static class WordCount{
		public String word;
		public long count;
		//记得要有这个空构造函数
		public WordCount(){

		}
		public WordCount(String word,long count){
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return "WordCount{" +
				"word='" + word + '\'' +
				", count=" + count +
				'}';
		}
	}
}
