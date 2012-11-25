package org.apache.hadoop.rabbit.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RabbitMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable>{

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
	}

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		//1. get value
		//2. spawn one thread to emit message to JT
		//3. core logic throw zk to shedule
		//4. has flag to notify if exit or not
	}

	@Override
	public void run(Context context)
			throws IOException, InterruptedException {
		super.run(context);
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}
	
}
