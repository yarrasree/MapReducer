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

public class Test {
 static Logger log = Logger.getLogger(Test.class.getName());
	/**
	 * @param args
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IllegalArgumentException,
			IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		System.out.println("Hello");
		log.info("Main Test driver program");
		/*
		 * Validate that two arguments were passed from the command line.
		 */
		if (args.length != 2) {
			System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
			System.exit(-1);
		}
		Configuration conf = new Configuration();
		//conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " "); //added for key valuetext input file format
		//conf.set("key.value.separator.in.input.line", ","); 
		conf.set("topN", "2");
		/*
		 * Instantiate a Job object for your job's configuration.
		 */
		Job job = new Job(conf);

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		// job.setJarByClass(MyDriver.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		job.setJobName("My Driver");
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		job.setJarByClass(Test.class);
		
		job.setMapperClass(StubMapper.class);
		job.setReducerClass(StubReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//input file format type
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		log.info("Main Test driver program end ");
	}

}
