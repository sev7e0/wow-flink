package com.sev7e0.wow.java.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Objects;

public class KeyedStateDemo {

	/**
	 * 输入 ：1 2
	 * 		 2 3
	 * 		 3 2
	 * 		 1 3
	 * 		 1 4
	 * 输出 ：6> (1,3.0)
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> socketTextStream = environment.socketTextStream("localhost", 9090);

		SingleOutputStreamOperator<Tuple2<Long, Long>> streamOperator = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<Long, Long>>() {
			@Override
			public void flatMap(String s, Collector<Tuple2<Long, Long>> collector) throws Exception {
				String[] s1 = s.split(" ");
				collector.collect(Tuple2.of(Long.valueOf(s1[0]),Long.valueOf(s1[1])));
			}
		});

		//使用kyBy产生keyedStream，之后才能够管理keyed state
		streamOperator.keyBy(0)
			.flatMap(new CountWindowAverageWithValueState()).print();

		environment.execute(KeyedStateDemo.class.getName());
	}
}

class CountWindowAverageWithValueState extends RichFlatMapFunction<Tuple2<Long,Long>, Tuple2<Long, Double>>{

	private ValueState<Tuple2<Long, Long>> value;

	@Override
	public void open(Configuration parameters) throws Exception {
		ValueStateDescriptor<Tuple2<Long, Long>> valueStateDescriptor = new ValueStateDescriptor<>("average",
			Types.TUPLE(Types.LONG, Types.LONG));
		//找到当前状态
		value = getRuntimeContext().getState(valueStateDescriptor);
	}

	@Override
	public void flatMap(Tuple2<Long, Long> longLongTuple2, Collector<Tuple2<Long, Double>> collector) throws Exception {

		//将valueState中的值取出
		Tuple2<Long, Long> valueStates = this.value.value();

		if (Objects.isNull(valueStates)){
			valueStates = Tuple2.of(0l, 0l);
		}

		valueStates.f1+=1;

		valueStates.f0+=longLongTuple2.f1;

		value.update(valueStates);

		if (valueStates.f1 ==3){

			double average = valueStates.f0 / valueStates.f1;

			collector.collect(Tuple2.of(longLongTuple2.f0,average));
			value.clear();

		}

	}

}