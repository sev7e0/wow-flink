package com.sev7e0.wow.java;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SingleSource implements SourceFunction<Long> {

	private Long number = 1L;
	boolean running = true;

	//implement run method for production data
	@Override
	public void run(SourceContext<Long> sourceContext) throws Exception {
		while (running){
			sourceContext.collect(number);
			number++;
			Thread.sleep(1000);
		}
	}

	//hook method for cancel production data
	@Override
	public void cancel() {
		running = false;
	}
}
