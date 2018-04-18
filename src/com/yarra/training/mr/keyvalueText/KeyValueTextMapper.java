package com.yarra.training.mr.keyvalueText;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class KeyValueTextMapper extends Mapper<Text, Text, Text, IntWritable> {
	IntWritable outputValue = new IntWritable(1);
	static Logger log = Logger.getLogger(KeyValueTextMapper.class.getName());
  @Override
  public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {
	  log.info("map ====> ");
	  Configuration conf = context.getConfiguration();
	  String headerLine = conf.get("HEADER_LINE");
	  log.info("Key is  ==> "+key.toString());
	  StringTokenizer stokenizer = new StringTokenizer(headerLine);
	  boolean headerRowFlag = false;
	  if (stokenizer.hasMoreTokens()) {
		  String headerKey = stokenizer.nextToken();
		  if (headerKey.equalsIgnoreCase(key.toString())){
			  headerRowFlag = true;
		  }
	  }
	  
	  log.info("Value is  ==> "+value.toString());
	  if (!headerRowFlag) {
		  IntWritable salesValue= new IntWritable(0);
		  try {
			  int intValue = 0;
			  if (value.getLength() > 0 ) {
				  intValue = Integer.parseInt(value.toString());  
			  }
			  salesValue = new IntWritable(intValue);
		  } catch(NumberFormatException ex) {
			  log.error("Number Format exception while converting text to int "+ex.getMessage());
		  }
		  catch(Exception ex) {
			  log.error("Exception while converting text to int "+ex.getMessage());
		  }
		  log.info("Sales Value is  ==> "+salesValue.get());
		  context.write(key, salesValue);
	  }

  }
}
