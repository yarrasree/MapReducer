import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class StubReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	static Logger log = Logger.getLogger(StubReducer.class.getName());
	int reduceCounter =0;
	HashMap<String,Integer>  topN = new HashMap<String,Integer>();
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	  log.info("reduce method============================>");
	  int sum = 0;
	  int funCount =0;
	  reduceCounter ++;
	   for(IntWritable value : values)
	   {   funCount++;
		   log.info(key.toString() +" reduce method inside for loop:"+value.get());
		   sum += value.get();
		   log.info(key.toString() +"  reduce method inside for loop 2 "+value.get());
	   }
	   //context.write(key, new IntWritable(sum));
	   topN.put(key.toString(), sum);
	   //topN.put("yarra"+reduceCounter, new IntWritable(reduceCounter));
	   log.info(reduceCounter+" reducer counter funCount "+funCount +" topN map is :"+topN);
	   log.info("reduce method is end ==============================>");
  }
  
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		log.info(reduceCounter+"cleanup method "+topN);
		HashMap<String,Integer> myMap = sortByValues(topN);
        int counter = 1;
        Configuration conf = context.getConfiguration();
        String topNValue = conf.get("topN");
        log.info("topNValue is  ============= > "+topNValue);
        
        int topValue = 0;
        try {
        	topValue = Integer.parseInt(topNValue);
        } catch(Exception ex) {
        	
        	log.error("Exception while parsing topn value "+ex.getMessage());
        }
        log.info("topNValue is  ============= > "+topValue);
        ArrayList<String> keys = new ArrayList<String>(myMap.keySet());
        for (int i=keys.size()-1;i>=0;i--) {
        	context.write(new Text(keys.get(i)), new IntWritable(myMap.get(keys.get(i))));
            if (counter++ == topValue) {
                break;
            }
           
        }
	}
	private HashMap<String,Integer> sortByValues(HashMap map) { 
	       List<String> list = new LinkedList<String>(map.entrySet());
	       log.info("enter of sortedMap : "+list);
	       // Defined Custom Comparator here
	       Collections.sort(list, new Comparator() {
	            public int compare(Object o1, Object o2) {
	            	log.info("compare method of sortbyValues");
	               return ((Comparable) ((Map.Entry) (o1)).getValue())
	                  .compareTo(((Map.Entry) (o2)).getValue());
	            }
	       });
	       log.info(list+"middle of sortedMap : "+map);
	       // Here I am copying the sorted list in HashMap
	       // using LinkedHashMap to preserve the insertion order
	       HashMap<String,Integer> sortedHashMap = new LinkedHashMap<String,Integer>();
	       for (Iterator it = list.iterator(); it.hasNext();) {
	    	   	  log.info("sorting in keep in sorted map of sortbyValues");
	              Map.Entry<String,Integer> entry = (Map.Entry) it.next();
	              sortedHashMap.put(entry.getKey(), entry.getValue());
	       } 
	       return sortedHashMap;
	  }

	class MyComparator implements Comparator <Object> {
		private Map<String,Integer> map;
		MyComparator (Map<String,Integer> mp) {
			this.map = mp;
		}

		@Override
		public int compare(Object obj1, Object obj2) {
			if (map.get(obj1) == map.get(obj2) ) {
				return 1;
			}
			return ((Integer)map.get(obj1).compareTo((Integer)map.get(obj2)));
		}
		
	}
	
	
  
  
}