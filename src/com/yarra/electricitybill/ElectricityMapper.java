package com.yarra.electricitybill;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ElectricityMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		super.map(key, value, context);
		String line = value.toString();
		String lasttoken = null;
		StringTokenizer s = new StringTokenizer(line, "\t");
		String year = s.nextToken();

		while (s.hasMoreTokens()) {
			lasttoken = s.nextToken();
		}

		int avgprice = Integer.parseInt(lasttoken);
		
		context.write(new Text(year), new IntWritable(avgprice));
	}

}
