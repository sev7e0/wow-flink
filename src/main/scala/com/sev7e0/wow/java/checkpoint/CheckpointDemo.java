package com.sev7e0.wow.java.checkpoint;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * flink checkpoint demo
 */

public class CheckpointDemo {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9090);

		CheckpointConfig checkpointConfig = env.getCheckpointConfig();

		//开启后在任务取消或者失败时，不会删除外部检查点
		checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		//ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION
		//该模式下，任务取消时会删除检查点，但当任务失败时会保留checkpoint state


		//返回当存在较新的save point时，作业恢复是否应回退到checkpoint。 default: false
		checkpointConfig.setPreferCheckpointForRecovery(true);

		//default: CheckpointingMode.EXACTLY_ONCE;
		checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

		//checkpoint周期 当前设定五秒一次
		checkpointConfig.setCheckpointInterval(5000);

		//超过当前设定的时间将会被丢弃此checkpoint
		checkpointConfig.setCheckpointTimeout(60000);

		//可以接收多少次checkpoint失败，default：0
		checkpointConfig.setTolerableCheckpointFailureNumber(0);

		env.setStateBackend(new FsStateBackend("file:///tmp/flink-checkpoint/",true));

		socketTextStream.print();

		env.execute(CheckpointDemo.class.getName());
	}
}
