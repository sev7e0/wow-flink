package com.sev7e0.wow.java.utils;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

public class TestSourceUtils implements SourceFunction<String> {

	FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

	/**
	 * 产生的数据流类型
	 */
	private String streamDataType;

	public TestSourceUtils(String streamDataType) {
		this.streamDataType = streamDataType;
	}

	@Override
	public void run(SourceContext<String> ctx) throws Exception {

		switch (streamDataType){
			case "disorder":{
				produceDisorderData(ctx);
				break;
			}
			case "order":{
				produceOrderData(ctx);
				break;
			}
			default:{
				produceOrderData(ctx);
			}
		}

	}

	@Override
	public void cancel() {

	}

	/**
	 * 产生有序数据
	 * @param ctx sourceContext
	 * @throws InterruptedException
	 */
	private void produceOrderData(SourceContext<String> ctx) throws InterruptedException {
		String currTime = String.valueOf(System.currentTimeMillis());
		while (Integer.parseInt(currTime.substring(currTime.length() - 4)) > 100) {
			currTime = String.valueOf(System.currentTimeMillis());
		}
		System.out.println("开始发送事件的时间：" + dateFormat.format(System.currentTimeMillis()));
		// 第 13 秒发送两个事件
		TimeUnit.SECONDS.sleep(13);
		ctx.collect("hadoop," + System.currentTimeMillis());
		// 产生了一个事件，但是由于网络原因，事件没有发送
		ctx.collect("hadoop," + System.currentTimeMillis());
		// 第 16 秒发送一个事件
		TimeUnit.SECONDS.sleep(3);
		ctx.collect("hadoop," + System.currentTimeMillis());
		TimeUnit.SECONDS.sleep(300);
	}

	/**
	 * 产生无序数据
	 * @param ctx sourceContext
	 * @throws InterruptedException
	 */
	private void produceDisorderData(SourceContext<String> ctx) throws InterruptedException {
		String currTime = String.valueOf(System.currentTimeMillis());
		while (Integer.parseInt(currTime.substring(currTime.length() - 4)) > 100) {
			currTime = String.valueOf(System.currentTimeMillis());
		}
		System.out.println("开始发送事件的时间：" + dateFormat.format(System.currentTimeMillis()));
		// 第 13 秒发送两个事件
		TimeUnit.SECONDS.sleep(13);
		ctx.collect("hadoop," + System.currentTimeMillis());
		// 产生了一个事件，但是由于网络原因，事件没有发送
		ctx.collect("hadoop," + (System.currentTimeMillis()+5000L));
		// 第 16 秒发送一个事件
		ctx.collect("hadoop," + System.currentTimeMillis());
		TimeUnit.SECONDS.sleep(300);
	}

}
