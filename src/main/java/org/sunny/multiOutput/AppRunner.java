package org.sunny.multiOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class AppRunner extends Configured implements Tool {

	public AppRunner() {
		super();		
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("fs.defaultFS", "file:///");
		conf.set("mapreduce.framework.name", "local");
		Job job = Job.getInstance(getConf());
        job.setJobName("Multi-output");
        
        //setting the class names
        job.setJarByClass(AppRunner.class);
        job.setMapperClass(MultiOutputMapper.class);
        //job.setReducerClass(TrivagoReducer.class);                
        job.setNumReduceTasks(0);
        
        //setting the output data type classes
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job, "text", LazyOutputFormat.class, NullWritable.class, Text.class);
        
        FileInputFormat.addInputPath(job, new Path("file:///home/sunny/code/classicmodels.db/orders"));        
        FileOutputFormat.setOutputPath(job, new Path("file:///home/sunny/code/classicmodels.db/output"));
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
	}
}
