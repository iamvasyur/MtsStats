package iam.hadooper.com;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VolumeReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	
	
	public Log LOG = LogFactory.getLog(VolumeReducer.class);

    public void reduce(Text key, Iterable<LongWritable> values,
            Context context)
    throws IOException, InterruptedException {
    	long wordCount = 0;
    	  Iterator<LongWritable> it=values.iterator();
    	  while (it.hasNext()) {	  
    	   wordCount += it.next().get();
    	  }
    	  context.write(key, new LongWritable(wordCount));
    	 }
    }
