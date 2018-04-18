package com.yarra.training.mr.keyvalueText;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.yarra.training.mr.utility.MRUtility;

public class KeyValueTextReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	static Logger log = Logger.getLogger(KeyValueTextReducer.class.getName());
	HashMap<String,Integer> topN = new HashMap<String,Integer>();
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	  log.info("reduce method");
	  for(IntWritable value : values){
		  topN.put(key.toString(), value.get());
		  log.info(key.toString()+"=================================================================================== "+value.get());
	  }
	   log.info(" end of reducer my map : "+topN);
	   //context.write(key, new IntWritable(maxValue));
  }
  
	/*  @Override
	public void run(org.apache.hadoop.mapreduce.Reducer.Context arg0)
			throws IOException, InterruptedException {
		super.run(arg0);
		log.info("run method of Reducer");
	}*/
	  
	 @Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		log.info("cleanup method of Reducer");
		HashMap<String,Integer> resultedMap = MRUtility.sortHashByValues(topN);
		String topNStr = context.getConfiguration().get("topN");
		int topNRecords = 0;
		try {
			if (topNStr!= null && topNStr.trim().length()>0) {
				topNRecords = Integer.parseInt(topNStr);
			}
		} catch(Exception ex) {
			log.error("Exception while parsing the String to integer "+ex.getMessage());
		}
		
		ArrayList<String> keyList = new ArrayList<String>(resultedMap.keySet());
		int counter = 1;
		for (int i=keyList.size()-1;i>=0;i--) {
			context.write(new Text(keyList.get(i)), new IntWritable(resultedMap.get(keyList.get(i))));
			if (topNRecords == counter++ ) break;
		}
	}
	  
	  
}