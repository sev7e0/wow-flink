package com.sev7e0.wow.java.sink;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class CustomMysqlSink extends RichSinkFunction {
	@Override
	public void setRuntimeContext(RuntimeContext t) {
		super.setRuntimeContext(t);
	}

	@Override
	public RuntimeContext getRuntimeContext() {
		return super.getRuntimeContext();
	}

	@Override
	public IterationRuntimeContext getIterationRuntimeContext() {
		return super.getIterationRuntimeContext();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
	}

	@Override
	public void close() throws Exception {
		super.close();
	}

	@Override
	public void invoke(Object value, Context context) throws Exception {

	}
}
