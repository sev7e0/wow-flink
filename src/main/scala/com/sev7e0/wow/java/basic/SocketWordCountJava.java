package com.sev7e0.wow.java.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


public class SocketWordCountJava {public static void main(String[] args) throws Exception {
	//flink提供的工具类，获取传递的参数
	ParameterTool parameterTool = ParameterTool.fromArgs(args);
	String hostname = parameterTool.get("hostname");
	int port = parameterTool.getInt("port");
	//步骤一：获取执行环境
	StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	//步骤二：获取数据源
	DataStreamSource<String> dataStreamSource = env.socketTextStream(hostname, port);
	//步骤三：执行逻辑操作
	DataStream<WordCount> wordAndOneStream = dataStreamSource.flatMap(new FlatMapFunction<String, WordCount>() {
		public void flatMap(String line, Collector<WordCount> out) {
			String[] fields = line.split(",");
			for (String word : fields) {
				out.collect(new WordCount(word, 1L));
			}
		}
	}).setParallelism(2);
	KeyedStream<WordCount, Tuple> word = wordAndOneStream.keyBy("word");
	SingleOutputStreamOperator<WordCount> count = word.timeWindow(Time.seconds(2), Time.seconds(1))//每隔1秒计算最近2秒
		.sum("count");
	//步骤四：结果打印
	count.print().setParallelism(2);
	//步骤五：任务启动
	env.execute("WindowWordCountJava");
}

	public static class WordCount{
		public String word;
		public long count;
		//记得要有这个空构建
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

