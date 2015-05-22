package org.sunny.multiOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class App {
	public static void main(String[] args) throws Exception {
		//Set the configuration to compress intermediate output here so that it does not 
		//interfere with unit-testing.
		Configuration conf = new Configuration();		
		int exitcode = ToolRunner.run(conf, new AppRunner(), args);
		System.exit(exitcode);
	}
}