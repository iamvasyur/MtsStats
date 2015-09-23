package iam.hadooper.com;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DeltaReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
	
	
	public Log LOG = LogFactory.getLog(DeltaReducer.class);

    public void reduce(IntWritable key, Iterable<IntWritable> values,
            Context context)
    throws IOException, InterruptedException {
    	int wordCount = 0;
    	  Iterator<IntWritable> it=values.iterator();
    	  while (it.hasNext()) {	  
    	   wordCount += it.next().get();
    	   LOG.info("++++++++++++++++++++++++++++++"+wordCount);
    	  }
    	  context.write(key, new IntWritable(wordCount));
    	 }
    }



