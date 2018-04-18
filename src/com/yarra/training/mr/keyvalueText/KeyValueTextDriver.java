package com.yarra.training.mr.keyvalueText;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class KeyValueTextDriver {
 static Logger log = Logger.getLogger(KeyValueTextDriver.class.getName());
	/**
	 * @param args
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws Exception {

		System.out.println("Hello");
		log.info("Main Test driver program");

		if (args.length != 2) {
			System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		conf.set("topN", "5");
		conf.set("HEADER_LINE", "Month-Year,Number of Tractor Sold");
		//conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " "); //added for key valuetext input file format
		conf.set("key.value.separator.in.input.line", "	"); 

		/*
		 * Instantiate a Job object for your job's configuration.
		 */
		Job job = new Job(conf);

		job.setJobName("Key Value File Text Driver");
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		job.setJarByClass(KeyValueTextDriver.class);
		
		job.setMapperClass(KeyValueTextMapper.class);
		job.setReducerClass(KeyValueTextReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//input file format type
		//job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		log.info("Main Test driver program end ");
	}

}
