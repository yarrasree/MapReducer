import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class StubMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	IntWritable outputValue = new IntWritable(1);
	static Logger log = Logger.getLogger(StubMapper.class.getName());
	int mapCounter=0;
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  log.info("map ====> ");
	  String line = value.toString();
	  StringTokenizer st = new StringTokenizer(line);
	  int funCount =0;
	  mapCounter++;
	  while(st.hasMoreTokens())
	  {    funCount++;
		   log.info("while ====> ");
	       Text outputKey = new Text(st.nextToken());
	       log.info("  ====> "+outputKey);
	       context.write(outputKey, outputValue);
	  }
	  log.info(mapCounter+" map funCount ====> "+funCount);
  }
}
