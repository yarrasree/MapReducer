package com.yarra.training.mr.sequenceFile;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

public class SequenceFileDriver {
 static Logger log = Logger.getLogger(SequenceFileDriver.class.getName());
	/**
	 * @param args
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws Exception {

		System.out.println("Hello");
		log.info("SequenceFileDriver program");

		if (args.length != 2) {
			System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		//conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " "); //added for key valuetext input file format
		//conf.set("key.value.separator.in.input.line", "	"); 

		/*
		 * Instantiate a Job object for your job's configuration.
		 */
		Job job = new Job(conf);

		job.setJobName("Sequence File Driver");
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		job.setJarByClass(SequenceFileDriver.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(SequenceFileMapper.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(0);
		//input file format type
		//job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
	
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//conpress out put
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		//SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.RECORD);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		log.info("Sequebce File driver program end ");
	}

}
