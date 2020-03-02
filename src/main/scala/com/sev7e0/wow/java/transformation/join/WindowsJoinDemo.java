package com.sev7e0.wow.java.transformation.join;

import com.alibaba.fastjson.JSON;
import com.sev7e0.wow.java.utils.FlinkKafkaUtils;
import com.sev7e0.wow.java.utils.Message;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Windows Join Demo 主要是根据两个流中相同的key进行join 生成新的消息内容。
 */
public class WindowsJoinDemo {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9090);
		DataStreamSource<String> kafkaSource = FlinkKafkaUtils.buildSource(env, "message-java");

		DataStream<String> joinStream = kafkaSource.join(socketTextStream)
			// lambda表达式可以食用，java 真香
			.where(new KeySelector<String, String>() {
				@Override
				public String getKey(String s) throws Exception {
					Message message = JSON.parseObject(s, Message.class);
					return message.getSender();
				}
			}).equalTo((KeySelector<String, String>) s -> s.split(" ")[0])
			// 使用滚动窗口-处理时间
			.window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
			.apply(new JoinFunction<String, String, String>() {
				@Override
				public String join(String s, String s2) throws Exception {
					Message message = JSON.parseObject(s, Message.class);
					String[] s1 = s2.split(" ");
					String newContent = message.getContent() +"----"+ s1[1];
					//将相同的发送者的消息合并
					return JSON.toJSONString(new Message(message.getSender(),
						message.getReceiver(),
						message.getTime(),
						newContent,
						message.getTags()));
				}
			});

		joinStream.print();
		env.execute(WindowsJoinDemo.class.getName());
	}

}
