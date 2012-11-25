package org.apache.hadoop.rabbit.util;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * Job utils
 */
public final class JobUtils {
	
	public static Path getTmpSubDir(String subDir){
		 subDir = subDir + "-" + new Date().getTime()+ "-" + ((int)(Math.random()*100))+ "-" + Thread.currentThread().getId();
		return new Path(RabbitUtils.getTmpDir(),subDir);
	}
	
	public static JobId getJobId(String prefix){
		return new JobId(prefix);
	}
	
	public static class JobBuilder {

		public static Job parseInputAndOutput(Tool tool,Configuration conf,String[] args) throws IOException{
			if(args.length != 2){
				printUsage(tool, "<input> <output>");
				return null;
			}
			String src = args[0];
			String dst = args[1];
			Job job = new Job(conf);
			job.setJarByClass(tool.getClass());
			FileInputFormat.addInputPath(job, new Path(src));
			FileOutputFormat.setOutputPath(job, new Path(dst));
			return job;
		}

		public static void printUsage(Tool tool, String extraArgsUsage) {
			System.err.printf("Usage: %s [genericOptions] %s\n\n", tool.getClass().getSimpleName(),extraArgsUsage);
		}
		
	}
	
	public static class JobId {

		private final String prefix;
		
		private final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

		public JobId(String prefix) {
			super();
			this.prefix = prefix;
		}
		
		public String getId(){
			return this.prefix + "@" + this.sdf.format(new Date()) +"-" + Thread.currentThread().getId();
		}
	}

}
