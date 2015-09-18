package iam.hadooper.com;

import java.io.IOException;
import java.util.regex.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperExample extends Mapper<Object, Text, Text, IntWritable> {
	
 private Text word = new Text();
 private final static IntWritable one = new IntWritable(1);

 private static final Log LOG = LogFactory.getLog(MapperExample.class);
 
  
 @Override
 public void map(Object key, Text value,
   Context contex) throws IOException, InterruptedException {
  // Break line into words for processing
  Pattern mediaRatePattern= Pattern.compile("<E179>(\\d+)</E179>");
  Matcher mediaRateMatcher= mediaRatePattern.matcher(value.toString());
  while (mediaRateMatcher.find()){
	  word.set(mediaRateMatcher.group(1));
	  contex.write(word, one);
  }
  }
 
}