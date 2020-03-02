package com.sev7e0.wow.java.utils;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class FlinkKafkaUtils {



	public static DataStreamSource<String> buildSource(StreamExecutionEnvironment env,String topic){
		return env.addSource(new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), getKafkaProperties()));
	}

	public static Properties getKafkaProperties(){
		return getKafkaProperties(ParameterTool.fromSystemProperties());
	}

	public static Properties getKafkaProperties(ParameterTool parameterTool){
		Properties props = new Properties();
		props.put("bootstrap.servers",parameterTool.get("bootstrap.servers","spark01:9092"));
		props.put("zookeeper.connect", parameterTool.get("zookeeper.connect","spark01:2181"));
		props.put("group.id", parameterTool.get("group-id","message-group"));
		props.put("key.deserializer", parameterTool.get("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")); //key 反序列化
		props.put("key.serializer", parameterTool.get("key.serializer","org.apache.kafka.common.serialization.StringSerializer")) ;//key 反序列化
		props.put("value.deserializer", parameterTool.get("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer"));
		props.put("value.serializer", parameterTool.get("value.serializer","org.apache.kafka.common.serialization.StringSerializer"));
		props.put("auto.offset.reset", parameterTool.get("auto.offset.reset","latest"));
		return props;
	}

	public static KafkaProducer<String,String> getProducer(Properties properties){
		return new KafkaProducer<>(properties);
	}

	public static void main(String[] args) throws InterruptedException {
		while(true){
			ProducerRecord<String, String> record = new ProducerRecord<>("message-java", JSON.toJSONString(Message.produceMessage()));
			getProducer(getKafkaProperties()).send(record);
			Thread.sleep(2000);
		}
	}

}
