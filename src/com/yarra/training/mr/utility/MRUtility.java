package com.yarra.training.mr.utility;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class MRUtility {
	static Logger log = Logger.getLogger(MRUtility.class.getName());
	public static HashMap<String,Integer> sortHashByValues(HashMap map) { 
	       List<String> list = new LinkedList<String>(map.entrySet());
	       log.info("enter of sortedMap : "+list);
	       // Defined Custom Comparator here
	       Collections.sort(list, new Comparator() {
	            public int compare(Object o1, Object o2) {
	               return ((Comparable) ((Map.Entry) (o1)).getValue())
	                  .compareTo(((Map.Entry) (o2)).getValue());
	            }
	       });
	       // copying the sorted list in HashMap
	       // using LinkedHashMap to preserve the insertion order
	       HashMap<String,Integer> sortedHashMap = new LinkedHashMap<String,Integer>();
	       for (Iterator it = list.iterator(); it.hasNext();) {
	              Map.Entry<String,Integer> entry = (Map.Entry) it.next();
	              sortedHashMap.put(entry.getKey(), entry.getValue());
	       } 
	       log.info("final sortedMap : "+sortedHashMap);
	       return sortedHashMap;
	  }

}
