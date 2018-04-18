package com.yarra.training.mr.sequenceFile;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FormatConverterMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}

}
