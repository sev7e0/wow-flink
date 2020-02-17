package com.sev7e0.wow.java.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

public class ParallelSource implements ParallelSourceFunction<Long> {

	private boolean running = true;
	private Long number = 1L;

	private Random random = new Random(1L);

	@Override
	public void run(SourceContext<Long> sourceContext) throws Exception {
		while (running){
			sourceContext.collect(number);
			number += random.nextLong();
			Thread.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}
