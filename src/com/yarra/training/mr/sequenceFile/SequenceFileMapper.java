package com.yarra.training.mr.sequenceFile;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class SequenceFileMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	IntWritable outputValue = new IntWritable(1);
	static Logger log = Logger.getLogger(SequenceFileMapper.class.getName());
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  log.info("map ====> ");
	  log.info(key.toString()+" <=key ,Value is  ==> "+value.toString());
	  context.write(key, value);
	
  }
}
