package iam.hadooper.com;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeltaMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

	private final static IntWritable delta = null;
	 private final static IntWritable one = new IntWritable(1);

	 private static final Log LOG = LogFactory.getLog(DeltaMapper.class);
	 
	  
	 @Override
	 public void map(Object key, Text value,
	   Context contex) throws IOException, InterruptedException {
	  // Break line into words for processing
	  Pattern mediaDurationPattern= Pattern.compile("<E177>(\\d+)</E177>");
	  Pattern mediaPacedPattern= Pattern.compile("<E178>(\\d+)</E178>");
	  Matcher mediaDurationMatcher= mediaDurationPattern.matcher(value.toString());
	  Matcher mediaPacedMatcher= mediaPacedPattern.matcher(value.toString());
	  while (mediaDurationMatcher.find() && mediaPacedMatcher.find()){
		  delta.set(Integer.parseInt(mediaDurationMatcher.group(1))-Integer.parseInt(mediaPacedMatcher.group(1)));
		  contex.write(delta, one);
	  }
	  }

}
